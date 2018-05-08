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
	"time"

	"github.com/grailbio/internal/testutil"
	"golang.org/x/net/http2"
)

func TestMutualHTTPS(t *testing.T) {
	// This is a really nasty way of testing what's going on here,
	// but we do want to test this property end-to-end.
	mux := new(http.ServeMux)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	port, err := getFreeTCPPort()
	if err != nil {
		t.Fatal(err)
	}

	temp, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()

	sys := new(System)
	sys.authority, err = newCertificateAuthority(filepath.Join(temp, "authority"))
	if err != nil {
		t.Fatal(err)
	}
	// Create a second, unrelated authority. Clients from this should not be able
	// to communicate with the first.
	authority, err := newCertificateAuthority(filepath.Join(temp, "authority2"))
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		sys.ListenAndServe(fmt.Sprintf(":%d", port), mux)
	}()
	time.Sleep(time.Second)

	config, _, err := authority.HTTPSConfig()
	transport := &http.Transport{TLSClientConfig: config}
	http2.ConfigureTransport(transport)
	client := &http.Client{Transport: transport}
	_, err = client.Get(fmt.Sprintf("https://localhost:%d/", port))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "remote error: tls: bad certificate") {
		t.Fatalf("bad error %v", err)
	}
}

func getFreeTCPPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}
