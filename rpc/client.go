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

type clientState struct {
	uid    uint64 // uid distinguishes multiple http.Clients to the same server.
	client *http.Client
}

// A Client invokes remote methods on RPC servers.
type Client struct {
	factory func() *http.Client
	prefix  string

	mu      sync.Mutex
	nextUID uint64
	clients map[string]clientState
}

// NewClient creates a new RPC client.  clientFactory is called to create a new
// http.Client object. It may be called repeatedly and concurrently. prefix is
// prepended to the service method when constructing an URL.
func NewClient(clientFactory func() *http.Client, prefix string) (*Client, error) {
	return &Client{
		factory: clientFactory,
		prefix:  prefix,
		clients: map[string]clientState{}}, nil
}

func (c *Client) getClient(addr string) clientState {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.clients[addr]
	if !ok {
		h = clientState{
			uid:    c.nextUID,
			client: c.factory(),
		}
		c.clients[addr] = h
		c.nextUID++
	}
	return h
}

func (c *Client) resetClient(addr string, prevUID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := c.clients[addr]
	if h.uid == prevUID {
		log.Error.Printf("resetting http client for %s", addr)
		h := clientState{
			uid:    c.nextUID,
			client: c.factory(),
		}
		c.clients[addr] = h
		c.nextUID++
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
		log.Debug.Printf("call %s %s %v", addr, serviceMethod, arg)
		defer func() {
			if err != nil {
				log.Debug.Printf("call error %s %s %v: %v", addr, serviceMethod, arg, err)
			} else {
				log.Debug.Printf("call ok %s %s %v = %v", addr, serviceMethod, arg, reply)
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
	resp, err := ctxhttp.Post(ctx, h.client, url, contentType, body)
	switch err {
	case nil:
	case context.DeadlineExceeded, context.Canceled:
		c.resetClient(addr, h.uid)
		return err
	default:
		c.resetClient(addr, h.uid)
		return errors.E(errors.Net, errors.Temporary, err)
	}
	respBody := resp.Body
	if InjectFailures {
		respBody = &rpcFaultInjector{label: fmt.Sprintf("%s(%s)", serviceMethod, addr), in: respBody}
	}
	switch arg := reply.(type) {
	case *io.ReadCloser:
		switch resp.StatusCode {
		case methodErrorCode:
			dec := gob.NewDecoder(respBody)
			defer respBody.Close()
			e := new(errors.Error)
			if err := dec.Decode(e); err != nil {
				return errors.E(errors.Invalid, errors.Temporary, "error while decoding error", err)
			}
			c.resetClient(addr, h.uid)
			return e
		case 200:
			*arg = respBody
		default:
			respBody.Close()
			c.resetClient(addr, h.uid)
			return errors.E(errors.Invalid, errors.Temporary, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
		return nil
	default:
		defer respBody.Close()
		dec := gob.NewDecoder(respBody)
		switch resp.StatusCode {
		case methodErrorCode:
			e := new(errors.Error)
			if err := dec.Decode(e); err != nil {
				return errors.E(errors.Invalid, errors.Temporary, "error while decoding error for "+serviceMethod, err)
			}
			c.resetClient(addr, h.uid)
			return e
		case 200:
			err := dec.Decode(reply)
			if err != nil {
				c.resetClient(addr, h.uid)
				err = errors.E(errors.Invalid, errors.Temporary, "error while decoding reply for "+serviceMethod, err)
			}
			return err
		default:
			c.resetClient(addr, h.uid)
			return errors.E(errors.Invalid, errors.Temporary, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
	}
}
