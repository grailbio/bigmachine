// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rpc

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grailbio/base/errors"
)

var fatalErr = errors.E(errors.Fatal)

func TestNetError(t *testing.T) {
	url, client := newTestClient(t)
	e := errors.E(errors.Net, "some network error")
	err := client.Call(context.Background(), url, "Test.ErrorError", e, nil)
	if err == nil {
		t.Error("expected error")
	} else if !errors.Is(errors.Remote, err) {
		t.Errorf("error %v is not a remote error", err)
	} else if !errors.Match(e, errors.Recover(err).Err) {
		t.Errorf("error %v does not match expected error %v", err, e)
	}
}

// TestClientError verifies that client errors (4XXs) are handled appropriately.
func TestClientError(t *testing.T) {
	url, client := newTestClient(t)
	// Cause a (client) error by using an int instead of a string argument. This is a
	// bad request that is not a temporary condition (i.e. should not be retried).
	var notAString int
	err := client.Call(context.Background(), url, "Test.Echo", notAString, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Match(fatalErr, err) {
		t.Errorf("error %v is not fatal", err)
	}
}

// TestEncodeError verifies that errors encoding arguments are handled
// appropriately.
func TestEncodeError(t *testing.T) {
	url, client := newTestClient(t)
	type teapot struct {
		// nolint: structcheck,unused
		unexported int
	}
	// teapot will cause an encoding error because it has no exported fields.
	err := client.Call(context.Background(), url, "Test.Echo", teapot{}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Match(fatalErr, err) {
		t.Errorf("error %v is not fatal", err)
	}
}

// TestReaderFuncArgError verifies that a (func() (io.Reader, error)) arg
// passed to Call that returns an error causes the appropriate error handling.
func TestReaderFuncArgError(t *testing.T) {
	url, client := newTestClient(t)
	makeReader := func() (io.Reader, error) {
		return nil, errors.E(errors.Fatal, "test error")
	}
	err := client.Call(context.Background(), url, "Test.Echo", makeReader, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Match(fatalErr, err) {
		t.Errorf("error %v is not fatal", err)
	}
}

// newTestClient returns the address of a server running the TestService and a
// client for calling that server.
func newTestClient(t *testing.T) (string, *Client) {
	t.Helper()
	srv := NewServer()
	if err := srv.Register("Test", new(TestService)); err != nil {
		t.Fatal(err)
	}
	httpsrv := httptest.NewServer(srv)
	client, err := NewClient(func() *http.Client { return httpsrv.Client() }, testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	return httpsrv.URL, client
}
