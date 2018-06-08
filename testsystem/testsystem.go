// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package testsystem implements a bigmachine system that's useful
// for testing. Unlike other system implementations,
// testsystem.System does not spawn new processes: instead, machines
// are launched inside of the same process.
package testsystem

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/rpc"
)

// System implements a bigmachine System for testing.
// Systems should be instantiated with New().
type System struct {
	server *httptest.Server
	done   chan struct{}
	b      *bigmachine.B
	exited bool

	mux   http.ServeMux
	index uint64
}

// New creates a new System that is ready for use.
func New() *System {
	sys := &System{done: make(chan struct{})}
	sys.server = httptest.NewServer(&sys.mux)
	return sys
}

// Exited tells whether exit has been called on (any) machine.
func (s *System) Exited() bool {
	return s.exited
}

// Shutdown tears down temporary resources allocated by this
// System.
func (s *System) Shutdown() {
	close(s.done)
	s.server.Close()
}

// Name returns the name of the system.
func (s *System) Name() string {
	return "testsystem"
}

// Init initializes the System.
func (s *System) Init(b *bigmachine.B) error {
	s.b = b
	return nil
}

// Main panics. It should not be called, provided a correct
// bigmachine implementation.
func (s *System) Main() error {
	panic("Main called on testsystem")
}

// HTTPClient returns an http.Client that can converse with
// servers created by this test system.
func (s *System) HTTPClient() *http.Client {
	return s.server.Client()
}

// ListenAndServe panics. It should not be called, provided a
// correct bigmachine implementation.
func (s *System) ListenAndServe(addr string, handler http.Handler) error {
	panic("ListenAndServe called on testsystem")
}

// Start starts and returns a new Machine. Each new machine is
// provided with a supervisor. The only difference between the
// behavior of a supervisor of a test machine and a regular machine
// is that the test machine supervisor does not exec the process, as
// this would break testing.
func (s *System) Start(_ context.Context, count int) ([]*bigmachine.Machine, error) {
	machines := make([]*bigmachine.Machine, count)
	for i := range machines {
		server := rpc.NewServer()
		server.Register("Supervisor", bigmachine.StartSupervisor(s.b, s, server))
		index := atomic.AddUint64(&s.index, 1)
		path := "/" + fmt.Sprint(index)
		s.mux.Handle(path+bigmachine.RpcPrefix, server)
		machines[i] = &bigmachine.Machine{
			Addr:     s.server.URL + path,
			Maxprocs: 1,
			NoExec:   true,
		}
	}
	return machines, nil
}

// Exit marks the system as exited.
func (s *System) Exit(int) {
	s.exited = true
}

// Maxprocs returns 1.
func (s *System) Maxprocs() int {
	return 1
}
