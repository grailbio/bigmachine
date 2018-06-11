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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/rpc"
)

type closeIdleTransport interface {
	CloseIdleConnections()
}

type machine struct {
	*bigmachine.Machine
	Cancel func()
	Server *httptest.Server
}

func (m *machine) Kill() {
	m.Cancel()
	m.Server.CloseClientConnections()
	m.Server.Close()
	m.Server.Listener.Close()
	m.Server.Config.SetKeepAlivesEnabled(false)
}

// System implements a bigmachine System for testing.
// Systems should be instantiated with New().
type System struct {
	// Machineprocs is the number of procs per machine.
	Machineprocs int

	// The following can optionally be specified to customize the behavior
	// of Bigmachine's keepalive mechanism.
	KeepalivePeriod, KeepaliveTimeout, KeepaliveRpcTimeout time.Duration

	done   chan struct{}
	b      *bigmachine.B
	exited bool

	client *http.Client

	mu       sync.Mutex
	cond     *sync.Cond
	machines []*machine
}

// New creates a new System that is ready for use.
func New() *System {
	s := &System{
		Machineprocs: 1,
		done:         make(chan struct{}),
		client:       &http.Client{Transport: &http.Transport{}},
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *System) Wait(n int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.machines) < n {
		s.cond.Wait()
	}
	return n
}

// N returns the number of live machines in the test system.
func (s *System) N() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.machines)
}

// KillRandom kills a random machine, returning true if it was successful.
func (s *System) KillRandom() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.machines) == 0 {
		return false
	}
	i := rand.Intn(len(s.machines))
	m := s.machines[i]
	s.machines = append(s.machines[:i], s.machines[i+1:]...)
	m.Kill()
	return true
}

// Kill kills the machine m that is under management of this system,
// returning true if successful.
func (s *System) Kill(m *bigmachine.Machine) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sm := range s.machines {
		if sm.Machine == m {
			s.machines = append(s.machines[:i], s.machines[i+1:]...)
			sm.Kill()
			return true
		}
	}
	return false
}

// Exited tells whether exit has been called on (any) machine.
func (s *System) Exited() bool {
	return s.exited
}

// Shutdown tears down temporary resources allocated by this
// System.
func (s *System) Shutdown() {
	close(s.done)

	if t, ok := http.DefaultTransport.(closeIdleTransport); ok {
		t.CloseIdleConnections()
	}
	if t, ok := s.client.Transport.(closeIdleTransport); ok {
		t.CloseIdleConnections()
	}
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
	return s.client
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
		ctx, cancel := context.WithCancel(context.Background())
		server := rpc.NewServer()
		server.Register("Supervisor", bigmachine.StartSupervisor(ctx, s.b, s, server))
		mux := http.NewServeMux()
		mux.Handle(bigmachine.RpcPrefix, server)
		httpServer := httptest.NewServer(mux)

		m := &bigmachine.Machine{
			Addr:     httpServer.URL,
			Maxprocs: s.Machineprocs,
			NoExec:   true,
		}
		s.mu.Lock()
		s.machines = append(s.machines, &machine{m, cancel, httpServer})
		s.cond.Broadcast()
		s.mu.Unlock()
		machines[i] = m
	}
	return machines, nil
}

// Exit marks the system as exited.
func (s *System) Exit(int) {
	s.exited = true
}

// Maxprocs returns 1.
func (s *System) Maxprocs() int {
	return s.Machineprocs
}

func (s *System) KeepaliveConfig() (period, timeout, rpcTimeout time.Duration) {
	if period = s.KeepalivePeriod; period == 0 {
		period = time.Minute
	}
	if timeout = s.KeepaliveTimeout; timeout == 0 {
		timeout = 2 * time.Minute
	}
	if rpcTimeout = s.KeepaliveRpcTimeout; rpcTimeout == 0 {
		rpcTimeout = 10 * time.Second
	}
	return
}
