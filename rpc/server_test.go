// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rpc

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/errors"
)

const testPrefix = "/"

var digester = digest.Digester(crypto.SHA256)

type TestService struct{}

func (s *TestService) Echo(ctx context.Context, arg string, reply *string) error {
	*reply = arg
	return nil
}

func (s *TestService) Error(ctx context.Context, message string, reply *string) error {
	return errors.E(message)
}

func (s *TestService) ErrorError(ctx context.Context, err *errors.Error, reply *string) error {
	return err
}

func TestServer(t *testing.T) {
	srv := NewServer()
	srv.Register("Test", new(TestService))
	httpsrv := httptest.NewServer(srv)
	client, err := NewClient(func() *http.Client { return httpsrv.Client() }, testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var reply string
	if err := client.Call(ctx, httpsrv.URL, "Test.Echo", "hello world", &reply); err != nil {
		t.Fatal(err)
	}
	if got, want := reply, "hello world"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	err = client.Call(ctx, httpsrv.URL, "Test.Error", "the error message", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(errors.Remote, err) {
		t.Errorf("expected remote error")
	}
	cause := errors.Recover(err).Err
	if cause == nil {
		t.Fatalf("expected remote error to have a cause")
	}
	if got, want := cause.Error(), "the error message"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// Just test that nil replies just discard the result.
	if err := client.Call(ctx, httpsrv.URL, "Test.Echo", "hello world", nil); err != nil {
		t.Error(err)
	}
	_, err = os.Open("/dev/notexist")
	e := errors.E(errors.Precondition, "xyz", err)
	err = client.Call(ctx, httpsrv.URL, "Test.ErrorError", e, nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(errors.Remote, err) {
		t.Errorf("expected remote error")
	}
	if !errors.Match(e, errors.Recover(err).Err) {
		t.Errorf("error %v does not match expected error %v", err, e)
	}
}

type TestStreamService struct{}

func (s *TestStreamService) Echo(ctx context.Context, arg io.Reader, reply *io.ReadCloser) error {
	r, w := io.Pipe()
	go func() {
		_, err := io.Copy(w, arg)
		w.CloseWithError(err)
	}()
	*reply = r
	return nil
}

func (s *TestStreamService) StreamWithError(ctx context.Context, errstr string, reply *io.ReadCloser) error {
	r, w := io.Pipe()
	w.CloseWithError(errors.New(errstr))
	*reply = r
	return nil
}

func (s *TestStreamService) Gimme(ctx context.Context, count int, reply *io.ReadCloser) error {
	r, w := io.Pipe()
	*reply = r
	go func() {
		block := make([]byte, 1024)
		for count > 0 {
			b := block
			if count < len(b) {
				b = b[:count]
			}
			n, err := w.Write(b)
			if err != nil {
				w.CloseWithError(err)
				return
			}
			count -= n
		}
		w.Close()
	}()
	return nil
}

func (s *TestStreamService) Digest(ctx context.Context, arg io.Reader, reply *digest.Digest) error {
	w := digester.NewWriter()
	if _, err := io.Copy(w, arg); err != nil {
		return err
	}
	*reply = w.Digest()
	return nil
}

func TestStream(t *testing.T) {
	srv := NewServer()
	srv.Register("Stream", new(TestStreamService))
	httpsrv := httptest.NewServer(srv)
	client, err := NewClient(func() *http.Client { return httpsrv.Client() }, testPrefix)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b := make([]byte, 1024)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	var rc io.ReadCloser
	if err := client.Call(ctx, httpsrv.URL, "Stream.Echo", bytes.NewReader(b), &rc); err != nil {
		t.Fatal(err)
	}
	c, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Error(err)
	}
	if !bytes.Equal(b, c) {
		t.Errorf("got %v, want %v", c, b)
	}

	// Make sure errors are propagated (both ways).
	if err := client.Call(ctx, httpsrv.URL, "Stream.StreamWithError", "a series of unfortunate events", &rc); err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(ioutil.Discard, rc); err == nil || !strings.Contains(err.Error(), "a series of unfortunate events") {
		t.Errorf("bad error %v", err)
	}
	rc.Close()

	var d digest.Digest
	if err := client.Call(ctx, httpsrv.URL, "Stream.Digest", bytes.NewReader(b), &d); err != nil {
		t.Fatal(err)
	}
	if got, want := d, digester.FromBytes(b); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	n := 32 << 20
	if err := client.Call(ctx, httpsrv.URL, "Stream.Gimme", n, &rc); err != nil {
		t.Fatal(err)
	}
	m, err := io.Copy(ioutil.Discard, rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Error(err)
	}
	if got, want := m, int64(n); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
