// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/fatbin"
	"github.com/grailbio/base/iofmt"
	"github.com/grailbio/base/limitbuf"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/bigmachine/rpc"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

// NumKeepaliveReplyTimes is the number of keepalive reply times to
// store for each machine.
const numKeepaliveReplyTimes = 10

// TODO(marius): We could define a Gob decoder for machines that
// encode its address and dial it on decode. On the other hand, it's
// nice to be explicit about dialling.
//
// TODO(marius): When an driver execs a new machine, we should give
// it an instance cookie that is included in the actual address.
// Thus, we can check that are talking to the actual intended
// instance and not just another machine that happens to run on the
// same address.

// RetryPolicy is the default retry policy used for machine calls.
var retryPolicy = retry.Backoff(time.Second, 5*time.Second, 1.5)

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
	System  mem.VirtualMemoryStat
	Runtime runtime.MemStats
}

// A DiskInfo describes system disk usage.
type DiskInfo struct {
	Usage disk.UsageStat
}

// A LoadInfo describes system load.
type LoadInfo struct {
	Averages load.AvgStat
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

	// Environ is the process environment to be propagated to the remote
	// process.
	environ []string

	owner bool

	client *rpc.Client
	cancel func()

	mu        sync.Mutex
	state     int64
	err       error
	waiters   []stateWaiter
	cancelers map[canceler]struct{}

	nextKeepalive       time.Time
	numKeepalive        int
	keepaliveReplyTimes [numKeepaliveReplyTimes]time.Duration

	// KeepalivePeriod, keepaliveTimeout, and keepaliveRpcTimeout configures
	// keepalive behavior.
	keepalivePeriod, keepaliveTimeout, keepaliveRpcTimeout time.Duration
}

// Owned tells whether this machine was created and is managed
// by this bigmachine instance.
func (m *Machine) Owned() bool {
	return m.owner
}

// KeepaliveReplyTimes returns a buffer up to the last
// numKeepaliveReplyTimes keepalive reply latencies,
// most recent first.
func (m *Machine) KeepaliveReplyTimes() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.keepaliveReplyTimes)
	if m.numKeepalive < n {
		n = m.numKeepalive
	}
	times := make([]time.Duration, n)
	for i := range times {
		times[i] = m.keepaliveReplyTimes[(m.numKeepalive-i-1)%n]
	}
	return times
}

// NextKeepalive returns the time at which the next keepalive
// request is due.
func (m *Machine) NextKeepalive() time.Time {
	m.mu.Lock()
	t := m.nextKeepalive
	m.mu.Unlock()
	return t
}

// Hostname returns the hostname portion of the machine's address.
func (m *Machine) Hostname() string {
	u, err := url.Parse(m.Addr)
	if err != nil {
		return "unknown"
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return u.Host
	}
	return host
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
// Go runtime memory stats are read if readMemStats is true.
func (m *Machine) MemInfo(ctx context.Context, readMemStats bool) (info MemInfo, err error) {
	err = m.Call(ctx, "Supervisor.MemInfo", readMemStats, &info)
	return
}

// DiskInfo returns the machine's disk usage information.
func (m *Machine) DiskInfo(ctx context.Context) (info DiskInfo, err error) {
	err = m.Call(ctx, "Supervisor.DiskInfo", struct{}{}, &info)
	return
}

// LoadInfo returns the machine's current load.
func (m *Machine) LoadInfo(ctx context.Context) (info LoadInfo, err error) {
	err = m.Call(ctx, "Supervisor.LoadInfo", struct{}{}, &info)
	return
}

// Cancel cancels all pending operations on machine m. The machine
// is stopped with an error of context.Canceled.
func (m *Machine) Cancel() {
	m.cancel()
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
	if m.keepalivePeriod == 0 {
		m.keepalivePeriod, m.keepaliveTimeout, m.keepaliveRpcTimeout = b.System().KeepaliveConfig()
	}
	m.cancelers = make(map[canceler]struct{})
	ctx := context.Background()
	ctx, m.cancel = context.WithCancel(ctx)
	go func() {
		// TODO(marius): fix tests that rely on this.
		var system System
		if b != nil {
			system = b.System()
		}
		m.loop(ctx, system)
		m.cancel()
	}()
}

func (m *Machine) setError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
	m.setState(Stopped)
	log.Error.Printf("%s: %v", m.Addr, err)
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

func (m *Machine) loop(ctx context.Context, system System) {
	m.setState(Starting)

	if m.owner {
		if system != nil {
			go func() {
				r, err := system.Tail(ctx, m)
				if err == nil {
					_, err = io.Copy(iofmt.PrefixWriter(os.Stderr, m.Addr+": "), r)
				}
				if err != nil && err != context.Canceled {
					log.Error.Printf("%s: tail: %s", m.Addr, err)
				}
			}()
		}
		if !m.NoExec {
			// If we're the owner, loop is called after the machine was started
			// by the underlying system. We first wait for the machine to come
			// up (we give it 2 minutes).
			if err := m.ping(ctx); err != nil {
				m.setError(err)
				return
			}
			// Give us some extra time now. This keepalive will die anyway
			// after we exec.
			keepaliveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := m.call(keepaliveCtx, "Supervisor.Keepalive", 10*time.Minute, nil); err != nil {
				log.Error.Printf("Keepalive %v: %v", m.Addr, err)
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
	}

	if err := m.ping(ctx); err != nil {
		m.setError(err)
		return
	}

	if !m.owner {
		// If we're not the owner, we maintain machine state
		// (up or down) by maintaining a periodic ping.
		m.setState(Running)
		for {
			start := time.Now()
			err := m.retryCall(ctx, m.keepaliveTimeout, m.keepaliveRpcTimeout, "Supervisor.Ping", 0, nil)
			if err != nil {
				m.errorf("ping failed after %s (timeout=%s, rpc timeout=%s): %v",
					time.Since(start), m.keepaliveTimeout, m.keepaliveRpcTimeout, err)
				return
			}
			time.Sleep(m.keepalivePeriod / 2)
		}
	}

	// If we're the owner, there's a bunch of additional setup to perform:
	//
	//	(1) instantiate the machine's services
	//	(2) duplicate the machine's standard output and error to our own
	//	(3) maintain a keepalive
	//	(4) take emergency pre-OOM heap profiles if the keepalive reply
	//	  indicates that we're close to machine death
	for name, iface := range m.services {
		if err := m.retryCall(ctx, 5*time.Minute, 25*time.Second, "Supervisor.Register", service{name, iface}, nil); err != nil {
			m.setError(errors.E(err, fmt.Sprintf("Supervisor.Register %s", name)))
			return
		}
	}

	if system != nil {
		// Note that this means that OOMs are detected only by the owner
		// process. This is probably ok in most cases, but we should also
		// consider adding a system status propagation mechanism, so that
		// there is a global notion of a system's status. Note that for applications
		// like Bigslice, this mechanism is sufficient since machine status
		// is maintained entirely by the coordinator/scheduler node.
		go m.tryMonitorOOMs(ctx, system)
	}

	// Switch to running state now that all of the services are registered.
	m.setState(Running)

	const keepalive = 5 * time.Minute
	for {
		start := time.Now()
		var reply keepaliveReply
		err := m.retryCall(ctx, m.keepaliveTimeout, m.keepaliveRpcTimeout, "Supervisor.Keepalive", keepalive, &reply)
		if err != nil {
			m.errorf("keepalive failed after %s (timeout=%s, rpc timeout=%s): %v",
				time.Since(start), m.keepaliveTimeout, m.keepaliveRpcTimeout, err)
			return
		}
		m.mu.Lock()
		m.keepaliveReplyTimes[m.numKeepalive%len(m.keepaliveReplyTimes)] = time.Since(start)
		m.numKeepalive++
		m.nextKeepalive = time.Now().Add(reply.Next)
		m.mu.Unlock()
		next := reply.Next
		if next > m.keepalivePeriod {
			next = m.keepalivePeriod
		}
		nextc := time.After(next / 2)

		// Check memory stats and take a heap profile if we're likely to die soon.
		//
		// TODO(marius): rate limit, collect, or rotate these?
		if !reply.Healthy {
			log.Printf("%s: supervisor indicated machine was unhealthy, taking heap profile and expvar dump", m.Addr)
			suffix := "." + m.Hostname() + "-" + time.Now().Format("20060102T150405")
			path := "heap" + suffix
			if err := m.saveProfile(ctx, "heap", path); err != nil {
				log.Error.Printf("%s: heap profile failed: %v", m.Addr, err)
			} else {
				log.Printf("%s: heap profile saved to %s", m.Addr, path)
			}
			path = "vars" + suffix
			if err = m.saveExpvars(ctx, path); err != nil {
				log.Error.Printf("%s: failed to retrieve expvars: %v", m.Addr, err)
			} else {
				log.Printf("%s: expvars saved to %s", m.Addr, path)
			}
		}

		select {
		case <-nextc:
		case <-ctx.Done():
			m.setError(ctx.Err())
			return
		}
	}
}

// tryMonitorOOMs attempts to monitor the kernel log for OOMs, and whether
// they pertain to the supervised process. If an OOM is detected, machine m
// is failed.
func (m *Machine) tryMonitorOOMs(ctx context.Context, system System) {
	var pid int
	if err := m.retryCall(ctx, 5*time.Minute, 25*time.Second, "Supervisor.Getpid", struct{}{}, &pid); err != nil {
		log.Error.Printf("%s: could not get pid: %v: cannot monitor for OOMs", m.Addr, err)
		return
	}
	r, err := system.Read(ctx, m, "/dev/kmsg")
	if err != nil {
		log.Error.Printf("%s: could not read kernel message buffer: %v: cannot monitor for OOMs", m.Addr, err)
		return
	}
	look := fmt.Sprintf("Out of memory: Kill process %d", pid)
	scan := bufio.NewScanner(r)
	for scan.Scan() {
		if log.At(log.Debug) {
			log.Debug.Printf("%s kmsg: %s", m.Addr, scan.Text())
		}
		if strings.Contains(scan.Text(), look) {
			m.setError(errors.E(errors.OOM, "bigmachine process killed by the kernel"))
		}
	}
	if err := scan.Err(); err != nil && err != context.Canceled {
		log.Error.Printf("%s: could not tail kernel message buffer: %v: cannot monitor for OOMs", m.Addr, err)
	}
}

func (m *Machine) ping(ctx context.Context) error {
	return m.retryCall(ctx, 9*time.Minute, 3*time.Minute, "Supervisor.Ping", 0, nil)
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
	self, err := fatbin.Self()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var info Info
	if err := m.call(ctx, "Supervisor.Info", struct{}{}, &info); err != nil {
		return err
	}
	// First set the correct environment and arguments.
	if err := m.call(ctx, "Supervisor.Setenv", m.environ, nil); err != nil {
		return err
	}
	if err := m.call(ctx, "Supervisor.Setargs", os.Args, nil); err != nil {
		return err
	}
	rc, err := self.Open(info.Goos, info.Goarch)
	if err != nil {
		return err
	}
	defer rc.Close()
	return m.call(ctx, "Supervisor.Exec", rc, nil)
}

func (m *Machine) call(ctx context.Context, serviceMethod string, arg, reply interface{}) (err error) {
	if log.At(log.Debug) {
		var deadline string
		if d, ok := ctx.Deadline(); ok {
			deadline = fmt.Sprintf(" [deadline:%s]", time.Until(d))
		}
		var call string
		if log.At(log.Debug) {
			call = fmt.Sprintf("%s %s(%v)", m.Addr, serviceMethod, truncatef(arg))
		}
		log.Debug.Print(call, deadline)
		defer func() {
			if err != nil {
				log.Debug.Print(call, " error: ", err)
			}
		}()
	}
	err = m.client.Call(ctx, m.Addr, serviceMethod, arg, reply)
	return err
}

func (m *Machine) retryCall(ctx context.Context, overallTimeout, rpcTimeout time.Duration, serviceMethod string, arg, reply interface{}) error {
	retryCtx, cancel := context.WithTimeout(ctx, overallTimeout)
	defer cancel()
	var err error
	for retries := 0; ; retries++ {
		rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		err = m.call(rpcCtx, serviceMethod, arg, reply)
		cancel()
		if err == nil {
			if retries > 0 {
				log.Printf("%s %s: succeeded after %d retries", m.Addr, serviceMethod, retries)
			}
			return nil
		}
		log.Debug.Printf("%s %s: %v; retrying (%d)", m.Addr, serviceMethod, err, retries)
		// TODO(marius): this isn't quite right. Introduce an errors package
		// similar to Reflow's here to categorize errors properly.
		if _, ok := err.(net.Error); !ok {
			log.Error.Printf("%s %s(%v): %v", m.Addr, serviceMethod, arg, err)
		}
		if err := retry.Wait(retryCtx, retryPolicy, retries); err != nil {
			// Change the severity from temporary -> fatal.
			return errors.E(errors.Fatal, err)
		}
	}
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
			defer cancel()
			err := m.call(ctx, serviceMethod, arg, reply)
			if err == nil || err != ctx.Err() || m.State() != Stopped {
				return err
			}
			fallthrough
		case Stopped:
			if err := m.Err(); err != nil {
				return errors.E(errors.Fatal, errors.Unavailable, err)
			}
			return errors.E(errors.Fatal, errors.Unavailable, fmt.Sprintf("machine %s stopped", m.Addr))
		default:
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-m.Wait(Running):
			}
		}
	}
}

// RetryCall invokes Call, and retries on a temporary error.
func (m *Machine) RetryCall(ctx context.Context, serviceMethod string, arg, reply interface{}) error {
	for retries := 0; ; retries++ {
		if err := m.Call(ctx, serviceMethod, arg, reply); err == nil || !errors.IsTemporary(err) {
			return err
		}
		if err := retry.Wait(ctx, retryPolicy, retries); err != nil {
			return errors.E(errors.Fatal, err)
		}
	}
}

// SaveProfile saves a profile to a local file. The name of the file is
// returned.
func (m *Machine) saveProfile(ctx context.Context, which, path string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var rc io.ReadCloser
	err := m.Call(ctx, "Supervisor.Profile", profileRequest{which, 0, false}, &rc)
	if err != nil {
		return err
	}
	defer rc.Close()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, rc)
	return err
}

// SaveExpvars saves a JSON-encoded snapshot of this machine's
// expvars to the provided path.
func (m *Machine) saveExpvars(ctx context.Context, path string) error {
	var vars Expvars
	if err := m.retryCall(ctx, 5*time.Minute, 25*time.Second, "Supervisor.Expvars", struct{}{}, &vars); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(vars)
}

func truncatef(v interface{}) string {
	b := limitbuf.NewLogger(512)
	fmt.Fprint(b, v)
	return b.String()
}
