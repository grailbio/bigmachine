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

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"golang.org/x/net/context/ctxhttp"
)

const gobContentType = "application/x-gob"

// A Client invokes remote methods on RPC servers.
type Client struct {
	client *http.Client
	prefix string
}

// NewClient creates a new RPC client using the provided HTTP client
// for dispatch. If client is nil, the default HTTP client is used.
func NewClient(client *http.Client, prefix string) (*Client, error) {
	return &Client{client: client, prefix: prefix}, nil
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
	resp, err := ctxhttp.Post(ctx, c.client, url, contentType, body)
	switch err {
	case nil:
	case context.DeadlineExceeded, context.Canceled:
		return err
	default:
		return errors.E(errors.Net, err)
	}
	switch arg := reply.(type) {
	case *io.ReadCloser:
		switch resp.StatusCode {
		case methodErrorCode:
			dec := gob.NewDecoder(resp.Body)
			defer resp.Body.Close()
			e := new(errors.Error)
			if err := dec.Decode(e); err != nil {
				return errors.E(errors.Invalid, "error while decoding error", err)
			}
			return e
		case 200:
			*arg = resp.Body
		default:
			resp.Body.Close()
			return errors.E(errors.Invalid, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
		return nil
	default:
		defer resp.Body.Close()
		dec := gob.NewDecoder(resp.Body)
		switch resp.StatusCode {
		case methodErrorCode:
			e := new(errors.Error)
			if err := dec.Decode(e); err != nil {
				return errors.E(errors.Invalid, "error while decoding error", err)
			}
			return e
		case 200:
			return dec.Decode(reply)
		default:
			return errors.E(errors.Invalid, fmt.Sprintf("%s: bad reply status %s", url, resp.Status))
		}
	}
}
