// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/bigmachine/rpc"
)

var traceFlag = flag.Bool("bigm.trace", false, "trace bigmachine RPCs")

// TODO(marius): We could define a Gob decoder for machines that
// encode its address and dial it on decode. On the other hand, it's
// nice to be explicit about dialling.
//
// TODO(marius): When an driver execs a new machine, we should give
// it an instance cookie that is included in the actual address.
// Thus, we can check that are talking to the actual intended
// instance and not just another machine that happens to run on the
// same address.

// OutputMu is used to safely interleave tail output from multiple machines.
var outputMu sync.Mutex

// State enumerates the possible states of a machine. Machine states
// proceed monotonically: they can only increase in value.
type State int32

const (
	// Unstarted indicates the machine has yet to be started.
	Unstarted State = iota
	// Starting indicates that the machine is currently bootstrapping.
	Starting
	// Running indicates that the machine is running and ready to
	// receive calls.
	Running
	// Stopped indicates that the machine was stopped, eitehr because of
	// a failure, or because the driver stopped it.
	Stopped
)

// String returns a State's string.
func (m State) String() string {
	switch m {
	case Unstarted:
		return "UNSTARTED"
	case Starting:
		return "STARTING"
	case Running:
		return "RUNNING"
	case Stopped:
		return "STOPPED"
	default:
		panic(fmt.Sprintf("invalid machine state %d", m))
	}
}

type stateWaiter struct {
	c     chan struct{}
	state State
}

type canceler interface {
	Cancel()
}

type cancelFunc struct{ cancel func() }

func (f *cancelFunc) Cancel() {
	f.cancel()
}

// A MemInfo describes system and Go runtime memory usage.
type MemInfo struct {
	Total, Free, Available data.Size
	MemStats               runtime.MemStats
}

func (m MemInfo) String() string {
	return fmt.Sprintf("total:%s free:%s available:%s", m.Total, m.Free, m.Available)
}

// A DiskInfo describes system disk usage.
type DiskInfo struct {
	Total, Free data.Size
}

func (d DiskInfo) String() string {
	return fmt.Sprintf("total:%s free:%s", data.Size(d.Total), data.Size(d.Free))
}

// A Machine is a single machine managed by bigmachine. Each machine
// is a "one-shot" execution of a bigmachine binary.  Machines embody
// a failure detection mechanism, but does not provide fault
// tolerance. Each machine comprises instances of each registered
// bigmachine service. A Machine is created by the bigmachine driver
// binary, but its address can be passed to other Machines which can
// in turn connect to each other (through Dial).
//
// Machines are created with (*B).Start.
type Machine struct {
	// Addr is the address of the machine. It may be used to create
	// machine instances through Dial.
	Addr string

	// Maxprocs is the number of processors available on the machine.
	Maxprocs int

	// NoExec should be set to true if the machine should not exec a
	// new binary. This is meant for testing purposes.
	NoExec bool

	// Services is the set of services to be instantiated on a new machine.
	services map[string]interface{}

	owner bool

	client *rpc.Client
	cancel func()

	mu        sync.Mutex
	state     int64
	err       error
	waiters   []stateWaiter
	cancelers map[canceler]struct{}
}

// State returns the machine's current state.
func (m *Machine) State() State {
	return State(atomic.LoadInt64(&m.state))
}

// Wait returns a channel that is closed once the machine reaches the
// provided state or greater.
func (m *Machine) Wait(state State) <-chan struct{} {
	c := make(chan struct{})
	m.mu.Lock()
	if state <= m.State() {
		close(c)
	} else {
		m.waiters = append(m.waiters, stateWaiter{c, state})
	}
	m.mu.Unlock()
	return c
}

// MemInfo returns the machine's memory usage information.
func (m *Machine) MemInfo(ctx context.Context) (info MemInfo, err error) {
	err = m.Call(ctx, "Supervisor.MemInfo", struct{}{}, &info)
	return
}

// DiskInfo returns the machine's disk usage information.
func (m *Machine) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	err = m.Call(ctx, "Supervisor.DiskInfo", struct{}{}, &info)
	return
}

// Err returns a machine's error. Err is only well-defined when the machine
// is in Stopped state.
func (m *Machine) Err() error {
	m.mu.Lock()
	err := m.err
	m.mu.Unlock()
	return err
}

func (m *Machine) start(b *B) {
	if m.client == nil {
		m.client = b.client
	}
	m.cancelers = make(map[canceler]struct{})
	go m.loop()
}

func (m *Machine) setError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
	m.setState(Stopped)
	log.Printf("%s: %v", m.Addr, err)
}

func (m *Machine) errorf(format string, args ...interface{}) {
	m.setError(fmt.Errorf(format, args...))
}

func (m *Machine) setState(s State) {
	m.mu.Lock()
	var triggered []chan struct{}
	ws := m.waiters
	m.waiters = nil
	for _, w := range ws {
		if w.state <= s {
			triggered = append(triggered, w.c)
		} else {
			m.waiters = append(m.waiters, w)
		}
	}
	atomic.StoreInt64(&m.state, int64(s))
	if s >= Stopped {
		for c := range m.cancelers {
			c.Cancel()
		}
		m.cancelers = make(map[canceler]struct{})
	}
	m.mu.Unlock()
	for _, c := range triggered {
		close(c)
	}
}

func (m *Machine) loop() {
	m.setState(Starting)
	ctx := context.Background()
	ctx, m.cancel = context.WithCancel(ctx)
	defer m.cancel()
	if m.owner && !m.NoExec {
		// If we're the owner, loop is called after the machine was started
		// by the underlying system. We first wait for the machine to come
		// up (we give it 2 minutes).
		if err := m.ping(ctx, 2*time.Minute); err != nil {
			m.setError(err)
			return
		}
		// Give us some extra time now. This keepalive will die anyway
		// after we exec.
		keepaliveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := m.call(keepaliveCtx, "Supervisor.Keepalive", 10*time.Minute, nil); err != nil {
			log.Printf("Keepalive %v: %v", m.Addr, err)
		}
		cancel()
		// Exec the current binary onto the machine. This will make the
		// machine unresponsive, because it will not have a chance to reply
		// to the exec call. We give it some time to recover.
		err := m.exec(ctx)
		// We expect an error since the process is execed before it has a chance
		// to reply. We check at least that the error comes from the right place
		// in the stack; other errors (e.g., context cancellations) result in a startup
		// failure.
		if err != nil && !errors.Is(errors.Net, err) {
			m.setError(err)
			return
		}
	}
	if err := m.ping(ctx, 2*time.Minute); err != nil {
		m.setError(err)
		return
	}

	if !m.owner {
		// If we're not the owner, we maintain machine state
		// (up or down) by maintaining a period ping.
		m.setState(Running)
		for {
			if err := m.ping(ctx, 5*time.Second); err != nil {
				m.errorf("ping failed: %v", err)
				return
			}
			time.Sleep(5 * time.Second)
		}
	}

	// If we're the owner, there's a bunch of additional setup to peform:
	//
	//	(1) instantiate the machine's services
	//	(2) duplicate the machine's standard output and error to our own
	//	(3) maintain a keepalive

	for name, iface := range m.services {
		if err := m.call(ctx, "Supervisor.Register", service{name, iface}, nil); err != nil {
			m.setError(errors.E(err, fmt.Sprintf("Supervisor.Register %s", name)))
			return
		}
	}

	// Switch to running state now that all of the services are registered.
	m.setState(Running)

	for _, fd := range []int{syscall.Stdout, syscall.Stderr} {
		go func(fd int) {
			if err := m.tail(ctx, fd); err != nil {
				log.Printf("tail %s %d: %v; no longer receiving logs from machine", m.Addr, fd, err)
			}
		}(fd)
	}
	const keepalive = time.Minute
	for {
		retryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var next time.Duration
		err := m.retryCall(retryCtx, newBackoffRetrier(time.Second, 1), "Supervisor.Keepalive", keepalive, &next)
		cancel()
		if err != nil {
			m.errorf("keepalive failed: %v", err)
			return
		}
		if next > 10*time.Second {
			next = 10 * time.Second
		}
		select {
		case <-time.After(next / 2):
		case <-ctx.Done():
			m.setError(ctx.Err())
			return
		}
	}
}

func (m *Machine) tail(ctx context.Context, fd int) error {
	var rc io.ReadCloser
	if err := m.call(ctx, "Supervisor.Tail", fd, &rc); err != nil {
		return err
	}
	defer rc.Close()
	b := bufio.NewScanner(rc)
	for b.Scan() {
		outputMu.Lock()
		fmt.Printf("%s: %s\n", m.Addr, b.Text())
		outputMu.Unlock()
	}
	return b.Err()
}

func (m *Machine) ping(ctx context.Context, maxtime time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, maxtime)
	defer cancel()
	return m.retryCall(ctx, newBackoffRetrier(time.Second, 1), "Supervisor.Ping", 0, nil)
}

// Context returns a new derived context that is canceled whenever
// the machine has stopped. This can be used to tie context lifetimes
// to machine lifetimes. The returned cancellation function should be
// called when the context is discarded.
func (m *Machine) context(ctx context.Context) (mctx context.Context, cancel func()) {
	ctx, ctxcancel := context.WithCancel(ctx)
	m.mu.Lock()
	if State(m.state) >= Stopped {
		m.mu.Unlock()
		ctxcancel()
		return ctx, func() {}
	}
	c := &cancelFunc{ctxcancel}
	m.cancelers[c] = struct{}{}
	m.mu.Unlock()
	return ctx, func() {
		m.mu.Lock()
		delete(m.cancelers, c)
		m.mu.Unlock()
	}
}

func (m *Machine) exec(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var info Info
	if err := m.call(ctx, "Supervisor.Info", struct{}{}, &info); err != nil {
		return err
	}
	if info.Goos != runtime.GOOS || info.Goarch != runtime.GOARCH {
		return fmt.Errorf("invalid binary: need %s %s, have %s %s",
			info.Goarch, info.Goos, runtime.GOARCH, runtime.GOOS)
	}
	// First set the correct arguments.
	if err := m.call(ctx, "Supervisor.Setargs", os.Args, nil); err != nil {
		return err
	}
	rc, err := binary()
	if err != nil {
		return err
	}
	defer rc.Close()
	return m.call(ctx, "Supervisor.Exec", rc, nil)
}

func (m *Machine) call(ctx context.Context, serviceMethod string, arg, reply interface{}) (err error) {
	if *traceFlag {
		var deadline string
		if d, ok := ctx.Deadline(); ok {
			deadline = fmt.Sprintf(" [deadline:%s]", time.Until(d))
		}
		log.Printf("%s %s(%v)%s", m.Addr, serviceMethod, arg, deadline)
		defer func() {
			if err != nil {
				log.Printf("%s %s(%v) error: %v", m.Addr, serviceMethod, arg, err)
			} else {
				log.Printf("%s %s(%v) ok %v", m.Addr, serviceMethod, arg, reply)
			}
		}()
	}
	return m.client.Call(ctx, m.Addr, serviceMethod, arg, reply)
}

func (m *Machine) retryCall(ctx context.Context, retrier retrier, serviceMethod string, arg, reply interface{}) error {
	for retrier.Next(ctx) {
		if err := m.call(ctx, serviceMethod, arg, reply); err != nil {
			// TODO(marius): this isn't quite right. Introduce an errors package
			// similar to Reflow's here to categorize errors properly.
			if _, ok := err.(net.Error); !ok {
				log.Printf("%s %s(%v): %v", m.Addr, serviceMethod, arg, err)
			}
		} else {
			return nil
		}
	}
	return retrier.Err()
}

// Call invokes a method named by a service on this machine. The
// argument and reply must be provided in accordance to bigmachine's
// RPC mechanism (see package docs or the docs of the rpc package).
// Call waits to invoke the method until the machine is in running
// state, and fails fast when it is stopped.
//
// If a machine fails its keepalive, pending calls are canceled.
func (m *Machine) Call(ctx context.Context, serviceMethod string, arg, reply interface{}) error {
	for {
		switch state := m.State(); state {
		case Running:
			ctx, cancel := m.context(ctx)
			err := m.call(ctx, serviceMethod, arg, reply)
			cancel()
			return err
		case Stopped:
			if err := m.Err(); err != nil {
				return err
			}
			return fmt.Errorf("machine %s is stopped", m.Addr)
		default:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-m.Wait(Running):
			}
		}
	}
}

// A retrier is an interface to abstract retry logic. It should be
// called as follows:
//
//	var r retrier = ...
//	for r.Next(ctx) {
//		if err := retriableWork(); err == nil || !isRetriableError(err) {
//			return err
//		}
//	}
//	return r.Err()
type retrier interface {
	// Next is called before each call. Next returns a boolean
	// indicating whether the call should proceed. Next may sleep, but
	// will respect the passed-in context.
	Next(ctx context.Context) bool
	// Err returns the retrier's error, generally because a retry budget is
	// exhausted or because the context passed to Next was canceled.
	Err() error
}

type backoffRetrier struct {
	n, factor int
	wait      time.Duration
	err       error
}

// NewBackoffRetrier returns a retrier that performs retries until the
// context passed into Next is canceled. The retrier initially waits for
// the amount of time specified by parameter initial; on each try this
// value is multiplied by the provided factor.
func newBackoffRetrier(initial time.Duration, factor int) retrier {
	return &backoffRetrier{
		wait:   initial,
		factor: factor,
	}
}

func (b *backoffRetrier) Next(ctx context.Context) bool {
	b.n++
	if b.n == 1 {
		return true
	}
	wait := b.wait
	b.wait *= time.Duration(b.factor)
	deadline, ok := ctx.Deadline()
	if ok && time.Until(deadline) < wait {
		b.err = errors.E(errors.Timeout, "ran out of time while waiting for retry")
		return false
	}
	select {
	case <-time.After(wait):
		return true
	case <-ctx.Done():
		b.err = ctx.Err()
		return false
	}
}

func (b *backoffRetrier) Err() error {
	return b.err
}
