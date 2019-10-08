// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rpc

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/limitbuf"
	"github.com/grailbio/base/log"
	"golang.org/x/net/context/ctxhttp"
	"golang.org/x/time/rate"
)

const (
	gobContentType = "application/x-gob"

	// We warn on RPC payloads above this size.
	largeRpcPayload = 64 << 20
)

// Loggers used to inform the user of large payloads, but without
// spamming them.
var (
	largeArgLogger   = &rateLimitingOutputter{rate.NewLimiter(rate.Every(time.Minute), 2), log.GetOutputter()}
	largeReplyLogger = &rateLimitingOutputter{rate.NewLimiter(rate.Every(time.Minute), 2), log.GetOutputter()}
)

// clientState stores the state of a single client to a single server;
// used to reset client connections when needed.
type clientState struct {
	addr    string
	factory func() *http.Client

	once   sync.Once
	cached *http.Client
}

func (c *clientState) init() {
	c.cached = c.factory()
}

func (c *clientState) Client() *http.Client {
	c.once.Do(c.init)
	return c.cached
}

// A Client invokes remote methods on RPC servers.
type Client struct {
	factory func() *http.Client
	prefix  string

	// Loggers contains a rate limiting logger per client;
	// use getLogger to retrieve it.
	loggers sync.Map // map[string]*rateLimitingOutputter

	mu      sync.Mutex
	clients map[string]*clientState
}

// NewClient creates a new RPC client.  clientFactory is called to create a new
// http.Client object. It may be called repeatedly and concurrently. prefix is
// prepended to the service method when constructing an URL.
func NewClient(clientFactory func() *http.Client, prefix string) (*Client, error) {
	return &Client{
		factory: clientFactory,
		prefix:  prefix,
		clients: make(map[string]*clientState),
	}, nil
}

func (c *Client) getClient(addr string) *clientState {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.clients[addr]
	if h == nil {
		h = &clientState{
			addr:    addr,
			factory: c.factory,
		}
		c.clients[addr] = h
	}
	return h
}

func (c *Client) resetClient(h *clientState, serviceMethod, reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clients[h.addr] == h {
		log.Outputf(c.getLogger(h.addr), log.Error, "resetting http client %s while calling to %s: %s", h.addr, serviceMethod, reason)
		if h.cached != nil {
			h.cached.CloseIdleConnections()
		}
		delete(c.clients, h.addr)
	}
}

func (c *Client) getLogger(addr string) *rateLimitingOutputter {
	v, ok := c.loggers.Load(addr)
	if ok {
		return v.(*rateLimitingOutputter)
	}
	v, _ = c.loggers.LoadOrStore(addr, &rateLimitingOutputter{rate.NewLimiter(rate.Every(time.Minute), 1), log.GetOutputter()})
	return v.(*rateLimitingOutputter)
}

// Call invokes a method on the server named by the provided address.
// The method syntax is "Service.Method": Service is the name of the
// registered service; Method names the method to invoke.
//
// The argument and reply are encoded in accordance with the
// description of the package docs.
//
// If the argument is an io.Reader, it is streamed directly to the
// server method. In this case, Call does not return until the data
// are fully streamed. If the reply is an *io.ReadCloser, the reply
// is streamed directly from the server method. In this case, Call
// returns once the stream is available, and the client is
// responsible for fully reading the data and closing the reader. If
// an error occurs while the response is streamed, the returned
// io.ReadCloser errors on read.
//
// Remote errors are decoded into *errors.Error and returned.
// (Non-*errors.Error errors are converted by the server.) The RPC
// client does not pass on errors of kind errors.Net; these are
// converted to errors.Other. This way, any error of the kind
// errors.Net is guaranteed to originate from the immediate call;
// they are never from the application.
func (c *Client) Call(ctx context.Context, addr, serviceMethod string, arg, reply interface{}) (err error) {
	done := clientstats.Start(addr, serviceMethod)
	var (
		requestBytes = -1
		replyBytes   = -1
	)
	defer func() {
		done(int64(requestBytes), int64(replyBytes), err)
	}()
	url := strings.TrimRight(addr, "/") + c.prefix + serviceMethod
	if log.At(log.Debug) {
		call := fmt.Sprint("call ", addr, " ", serviceMethod, " ", truncatef(arg))
		log.Debug.Print(call)
		defer func() {
			if err != nil {
				log.Debug.Print(call, " error: ", err)
			} else {
				log.Debug.Print(call, " ok: ", truncatef(reply))
			}
		}()
	}
	var (
		body        io.Reader
		contentType string
	)
	switch arg := arg.(type) {
	case io.Reader:
		body = arg
		contentType = "application/octet-stream"
	default:
		b := new(bytes.Buffer)
		enc := gob.NewEncoder(b)
		if err := enc.Encode(arg); err != nil {
			// Because we are writing into a Buffer, any error we see is a
			// failure to encode, which will not succeed on retry without
			// intervention.
			return errors.E(errors.Fatal, errors.Invalid, err)
		}
		requestBytes = b.Len()
		if requestBytes > largeRpcPayload {
			log.Outputf(largeArgLogger, log.Info, "call %s %s: large argument: %d bytes", addr, serviceMethod, requestBytes)
		}
		body = b
		contentType = gobContentType
	}

	h := c.getClient(addr)
	resp, err := ctxhttp.Post(ctx, h.Client(), url, contentType, body)
	switch err {
	case nil:
	case context.DeadlineExceeded, context.Canceled:
		c.resetClient(h, serviceMethod, "deadline exceeded or cancelled")
		return err
	default:
		c.resetClient(h, serviceMethod, "temporary network error")
		return errors.E(errors.Net, errors.Temporary, err)
	}
	if InjectFailures {
		resp.Body = &rpcFaultInjector{label: fmt.Sprintf("%s(%s)", serviceMethod, addr), in: resp.Body}
	}
	switch arg := reply.(type) {
	case *io.ReadCloser:
		switch {
		case resp.StatusCode == methodErrorCode:
			dec := gob.NewDecoder(resp.Body)
			defer resp.Body.Close()
			return decodeError(serviceMethod, dec)
		case resp.StatusCode == 200:
			// Wrap the actual response in a stream reader so that
			// errors are propagated properly.
			*arg = streamReader{resp}
		case 400 <= resp.StatusCode && resp.StatusCode < 500:
			body, err := ioutil.ReadAll(resp.Body)
			// Nothing to do if closing fails.
			_ = resp.Body.Close()
			c.resetClient(h, serviceMethod, fmt.Sprintf("%s: client error %s, %v, %v", url, resp.Status, string(body), err))
			return errors.E(errors.Fatal, errors.Invalid, fmt.Sprintf("%s: client error %s", url, resp.Status))
		default:
			body, err := ioutil.ReadAll(resp.Body)
			// Nothing to do if closing fails.
			_ = resp.Body.Close()
			c.resetClient(h, serviceMethod, fmt.Sprintf("%s: bad reply status %s, %v, %v", url, resp.Status, string(body), err))
			return errors.E(errors.Invalid, errors.Temporary, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
		return nil
	default:
		defer resp.Body.Close()
		sizeReader := &sizeTrackingReader{Reader: resp.Body}
		dec := gob.NewDecoder(sizeReader)
		switch {
		case resp.StatusCode == methodErrorCode:
			return decodeError(serviceMethod, dec)
		case resp.StatusCode == 200:
			err := dec.Decode(reply)
			if err != nil {
				c.resetClient(h, serviceMethod, "error decoding reply")
				err = errors.E(errors.Invalid, errors.Temporary, "error while decoding reply for "+serviceMethod, err)
			}
			replyBytes = sizeReader.Len()
			if replyBytes > largeRpcPayload {
				log.Outputf(largeReplyLogger, log.Info, "call %s %s: large reply: %d bytes", addr, serviceMethod, replyBytes)
			}
			return err
		case 400 <= resp.StatusCode && resp.StatusCode < 500:
			body, err := ioutil.ReadAll(resp.Body)
			c.resetClient(h, serviceMethod, fmt.Sprintf("%s: client error %s, %v, %v", url, resp.Status, string(body), err))
			return errors.E(errors.Fatal, errors.Invalid, fmt.Sprintf("%s: client error %s", url, resp.Status))
		default:
			body, err := ioutil.ReadAll(resp.Body)
			c.resetClient(h, serviceMethod, fmt.Sprintf("%s: bad reply status %s, %v, %v", url, resp.Status, string(body), err))
			return errors.E(errors.Invalid, errors.Temporary, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
	}
}

// StreamReader reads a bigmachine byte stream, propagating
// any errors that may be set in a response's trailer.
type streamReader struct{ *http.Response }

func (r streamReader) Read(p []byte) (n int, err error) {
	n, err = r.Body.Read(p)
	if err != io.EOF {
		return n, err
	}
	if e := r.Trailer.Get(bigmachineErrorTrailer); e != "" {
		err = errors.New(e)
	}
	return n, err
}

func (r streamReader) Close() error {
	return r.Body.Close()
}

func truncatef(v interface{}) string {
	b := limitbuf.NewLogger(512)
	fmt.Fprint(b, v)
	return b.String()
}

// decodeErrors decodes a serialized error from the codec stream dec. It wraps
// errors with an errors.Remote so that callers can distinguish between errors
// in the machinery to execute the RPC and errors returned by the RPC itself.
func decodeError(serviceMethod string, dec *gob.Decoder) error {
	e := new(errors.Error)
	if err := dec.Decode(e); err != nil {
		return errors.E(errors.Invalid, errors.Temporary, "error while decoding error for "+serviceMethod, err)
	}
	return errors.E(errors.Remote, e)
}
