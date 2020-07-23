// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2system

import (
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigmachine/internal/authority"
	"github.com/grailbio/testutil"
	"golang.org/x/net/http2"
)

func TestDiskConfig(t *testing.T) {
	for _, test := range []struct {
		dataspace uint
		nslice    int
		sliceSize int64
	}{
		{1000, 3, 335},
		{5350, 16, 335},
		{5350 + 25, 17, 335},
		{6000, 18, 335},
	} {
		sys := System{Dataspace: test.dataspace}
		nslice, sliceSize := sys.sliceConfig()
		if got, want := nslice, test.nslice; got != want {
			t.Errorf("%+v: got %v, want %v", test, got, want)
		}
		if got, want := sliceSize, test.sliceSize; got != want {
			t.Errorf("%+v: got %v, want %v", test, got, want)
		}
	}
}

func TestMutualHTTPS(t *testing.T) {
	save := useInstanceIDSuffix
	useInstanceIDSuffix = false
	defer func() {
		useInstanceIDSuffix = save
	}()
	// This is a really nasty way of testing what's going on here,
	// but we do want to test this property end-to-end.
	mux := new(http.ServeMux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	temp, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	var err error
	sys := new(System)
	sys.authority, err = authority.New(filepath.Join(temp, "authority"))
	if err != nil {
		t.Fatal(err)
	}
	// Create a second, unrelated authority. Clients from this should not be able
	// to communicate with the first.
	authority, err := authority.New(filepath.Join(temp, "authority2"))
	if err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	var listenAndServeError errors.Once
	go func() {
		listenAndServeError.Set(sys.Serve(l, mux))
	}()

	config, _, err := authority.HTTPSConfig()
	transport := &http.Transport{TLSClientConfig: config}
	http2.ConfigureTransport(transport)
	client := &http.Client{Transport: transport}
	_, err = client.Get(fmt.Sprintf("https://%s/", l.Addr().String()))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "remote error: tls: bad certificate") {
		t.Fatalf("bad error %v", err)
	}
	if err := listenAndServeError.Err(); err != nil {
		t.Fatal(err)
	}
}
