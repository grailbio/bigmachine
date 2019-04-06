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
	"net/http"
	"strings"
	"sync"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"golang.org/x/net/context/ctxhttp"
)

const gobContentType = "application/x-gob"

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

func (c *Client) resetClient(h *clientState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clients[h.addr] == h {
		log.Error.Printf("resetting http client for %s", h.addr)
		delete(c.clients, h.addr)
	}
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
func (c *Client) Call(ctx context.Context, addr, serviceMethod string, arg, reply interface{}) (err error) {
	done := clientstats.Start(addr, serviceMethod)
	defer func() {
		if err == nil {
			// Only register successful replies (currently).
			done()
		}
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
			return errors.E(errors.Invalid, err)
		}
		body = b
		contentType = gobContentType
	}

	h := c.getClient(addr)
	resp, err := ctxhttp.Post(ctx, h.Client(), url, contentType, body)
	switch err {
	case nil:
	case context.DeadlineExceeded, context.Canceled:
		c.resetClient(h)
		return err
	default:
		c.resetClient(h)
		return errors.E(errors.Net, errors.Temporary, err)
	}
	if InjectFailures {
		resp.Body = &rpcFaultInjector{label: fmt.Sprintf("%s(%s)", serviceMethod, addr), in: resp.Body}
	}
	switch arg := reply.(type) {
	case *io.ReadCloser:
		switch resp.StatusCode {
		case methodErrorCode:
			dec := gob.NewDecoder(resp.Body)
			defer resp.Body.Close()
			e := new(errors.Error)
			if err := dec.Decode(e); err != nil {
				return errors.E(errors.Invalid, errors.Temporary, "error while decoding error", err)
			}
			c.resetClient(h)
			return e
		case 200:
			// Wrap the actual response in a stream reader so that
			// errors are propagated properly.
			*arg = streamReader{resp}
		default:
			resp.Body.Close()
			c.resetClient(h)
			return errors.E(errors.Invalid, errors.Temporary, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
		return nil
	default:
		defer resp.Body.Close()
		dec := gob.NewDecoder(resp.Body)
		switch resp.StatusCode {
		case methodErrorCode:
			e := new(errors.Error)
			if err := dec.Decode(e); err != nil {
				return errors.E(errors.Invalid, errors.Temporary, "error while decoding error for "+serviceMethod, err)
			}
			c.resetClient(h)
			return e
		case 200:
			err := dec.Decode(reply)
			if err != nil {
				c.resetClient(h)
				err = errors.E(errors.Invalid, errors.Temporary, "error while decoding reply for "+serviceMethod, err)
			}
			return err
		default:
			c.resetClient(h)
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
	var b bytes.Buffer
	fmt.Fprint(&b, v)
	if b.Len() > 512 {
		b.Truncate(512)
		b.WriteString("(truncated)")
	}
	return b.String()
}
