// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"expvar"
	"fmt"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	// Sha256 is imported because we use its implementation for
	// fingerprinting binaries.
	_ "crypto/sha256"
	"os"
	"sync"

	"github.com/grailbio/base/diagnostic/dump"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/rpc"
	"golang.org/x/sync/errgroup"
)

// RpcPrefix is the path prefix used to serve RPC requests.
const RpcPrefix = "/bigrpc/"

// B is a bigmachine instance. Bs are created by Start and, outside
// of testing situations, there is exactly one per process.
type B struct {
	system System
	// index is a process-unique identifier for this B. It is useful for
	// distinguishing logs or diagnostic information.
	index int32
	// name is a human-usable name for this B that can be provided by clients.
	// Like index, it is useful for distinguishing logs or diagnostic
	// information, but may be set to something contextually meaningful.
	name string

	server *rpc.Server
	client *rpc.Client

	mu       sync.Mutex
	machines map[string]*Machine
	driver   bool
	running  bool
}

// Option is an option that can be provided when starting a new B. It is a
// function that can modify the b that will be returned by Start.
type Option func(b *B)

// Name is an option that will name the B. See B.name.
func Name(name string) Option {
	return func(b *B) {
		b.name = name
	}
}

// nextBIndex is the index of the next B that is started.
var nextBIndex int32

// Start is the main entry point of bigmachine. Start starts a new B
// using the provided system, returning the instance. B's shutdown
// method should be called to tear down the session, usually in a
// defer statement from the program's main:
//
//	func main() {
//		// Parse flags, configure system.
//		b := bigmachine.Start()
//		defer b.Shutdown()
//
//		// bigmachine driver code
//	}
func Start(system System, opts ...Option) *B {
	b := &B{
		index:    atomic.AddInt32(&nextBIndex, 1) - 1,
		system:   system,
		machines: make(map[string]*Machine),
	}
	for _, opt := range opts {
		opt(b)
	}
	b.run()
	// Test systems run in a single process space and thus
	// expvar would panic with duplicate key errors.
	//
	// TODO(marius): allow multiple sessions to share a single expvar
	if system.Name() != "testsystem" && expvar.Get("machines") == nil {
		expvar.Publish("machines", &machineVars{b})
	}

	if system.Name() != "testsystem" {
		pfx := fmt.Sprintf("bigmachine-%02d-", b.index)
		if b.name != "" {
			pfx += fmt.Sprintf("%s-", b.name)
		}
		dump.Register(pfx+"status", makeStatusDumpFunc(b))
		dump.Register(pfx+"pprof-goroutine", makeProfileDumpFunc(b, "goroutine", 2))
		// TODO(jcharumilind): Should the heap profile do a gc?
		dump.Register(pfx+"pprof-heap", makeProfileDumpFunc(b, "heap", 0))
		dump.Register(pfx+"pprof-mutex", makeProfileDumpFunc(b, "mutex", 1))
		dump.Register(pfx+"pprof-profile", makeProfileDumpFunc(b, "profile", 0))
	}
	return b
}

// System returns this B's System implementation.
func (b *B) System() System { return b.system }

// IsDriver is true if this is a driver instance (rather than a spawned machine).
func (b *B) IsDriver() bool { return b.driver }

// Run is the entry point for bigmachine. When run is called by the
// driver process, it returns immediately; it never returns when
// called by machines.
//
// When run is called on a machine, it sets up the machine's
// supervisor and RPC server according to the System implementation.
// Run never returns when called from a machine.
func (b *B) run() {
	switch mode := os.Getenv("BIGMACHINE_MODE"); mode {
	case "":
		b.driver = true
	case "machine":
	default:
		log.Fatalf("invalid bigmachine mode %s", mode)
	}
	if err := b.system.Init(b); err != nil {
		log.Fatal(err)
	}
	var err error
	b.client, err = rpc.NewClient(func() *http.Client { return b.system.HTTPClient() }, RpcPrefix)
	if err != nil {
		log.Fatal(err)
	}
	b.mu.Lock()
	b.running = true
	b.mu.Unlock()
	if b.driver {
		return
	}
	b.server = rpc.NewServer()
	supervisor := StartSupervisor(context.Background(), b, b.system, b.server)
	b.server.Register("Supervisor", supervisor)
	if err := maybeInit(supervisor, b); err != nil {
		log.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle(RpcPrefix, b.server)
	go func() {
		log.Fatal(b.system.ListenAndServe("", mux))
	}()
	log.Fatal(b.system.Main())
	panic("not reached")
}

// Dial connects to the machine named by the provided address.
//
// The returned machine is not owned: it is not kept alive as Start
// does.
func (b *B) Dial(ctx context.Context, addr string) (*Machine, error) {
	// TODO(marius): normalize addrs?
	// TODO(marius): We should also embed some sort of cookie/capability
	// into the address so we can distinguish between different
	// instances of a machine on the same address.
	b.mu.Lock()
	m := b.machines[addr]
	if m == nil {
		m = &Machine{Addr: addr, owner: false}
		b.machines[addr] = m
		m.start(b)
		go func() {
			<-m.Wait(Stopped)
			log.Error.Printf("%s: machine stopped with error %s", m.Addr, m.Err())
			b.mu.Lock()
			delete(b.machines, addr)
			b.mu.Unlock()
		}()
	}
	b.mu.Unlock()
	return m, nil
}

// A Param is a machine parameter. Parameters customize machines
// before the are started.
type Param interface {
	applyParam(*Machine)
}

// Services is a machine parameter that specifies the set of services
// that should be served by the machine. Each machine should have at
// least one service. Multiple Services parameters may be passed.
type Services map[string]interface{}

func (s Services) applyParam(m *Machine) {
	if m.services == nil {
		m.services = make(map[string]interface{})
	}
	for name, iface := range s {
		m.services[name] = iface
	}
}

// Environ is a machine parameter that amends the process environment
// of the machine. It is a slice of strings in the form "key=value"; later
// definitions override earlies ones.
type Environ []string

func (e Environ) applyParam(m *Machine) {
	m.environ = append(m.environ, e...)
}

// Start launches up to n new machines and returns them. The machines are
// configured according to the provided parameters. Each machine must
// have at least one service exported, or else Start returns an
// error. The new machines may be in Starting state when they are
// returned. Start maintains a keepalive to the returned machines,
// thus tying the machines' lifetime with the caller process.
//
// Start returns at least one machine, or else an error.
func (b *B) Start(ctx context.Context, n int, params ...Param) ([]*Machine, error) {
	machines, err := b.system.Start(ctx, n)
	if err != nil {
		return nil, err
	}
	if len(machines) == 0 {
		return nil, errors.E(errors.Unavailable, "no machines started")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, m := range machines {
		for _, p := range params {
			p.applyParam(m)
		}
		if len(m.services) == 0 {
			return nil, errors.E(errors.Invalid, "no services provided")
		}
		m.owner = true
		m.start(b)
		b.machines[m.Addr] = m
	}
	return machines, nil
}

// Machines returns a snapshot of the current set machines known to this B.
func (b *B) Machines() []*Machine {
	b.mu.Lock()
	snapshot := make([]*Machine, 0, len(b.machines))
	for _, machine := range b.machines {
		snapshot = append(snapshot, machine)
	}
	b.mu.Unlock()
	return snapshot
}

// HandleDebug registers diagnostic http endpoints on the provided ServeMux.
func (b *B) HandleDebug(mux *http.ServeMux) {
	b.HandleDebugPrefix("/debug/bigmachine/", mux)
}

// HandleDebugPrefix registers diagnostic http endpoints on the provided
// ServeMux under the provided prefix.
func (b *B) HandleDebugPrefix(prefix string, mux *http.ServeMux) {
	mux.HandleFunc(prefix+"pprof/", b.pprofIndex)
	mux.Handle(prefix+"status", &statusHandler{b})
}

var indexTmpl = template.Must(template.New("index").Parse(`<html>
<head>
<title>/debug/bigmachine/pprof</title>
</head>
<body>
/debug/bigmachine/pprof<br>
merged:<br>
<table>
{{range .All}}
<tr><td align=right>{{.Count}}<td><a href="{{.Name}}?debug=1">{{.Name}}</a>
{{end}}
</table>
<br>
<a href="goroutine?debug=2">full goroutine stack dump</a>
<br><br>
{{range $mach, $stats := .Machines}}
{{$mach}}:<br>
<table>
{{range $stats}}
<tr><td align=right>{{.Count}}<td><a href="{{.Name}}?debug=1&machine={{$mach}}">{{.Name}}</a>
{{end}}
</table>
<br>
<a href="goroutine?debug=2&machine={{$mach}}">full goroutine stack dump</a>
<br><br>
{{end}}

</body>
</html>
`))

func (b *B) pprofIndex(w http.ResponseWriter, r *http.Request) {
	which := strings.TrimPrefix(r.URL.Path, "/debug/bigmachine/pprof/")
	if which != "" {
		handler := profileHandler{b, which}
		handler.ServeHTTP(w, r)
		return
	}

	var (
		stats    = make(map[string][]profileStat)
		g, ctx   = errgroup.WithContext(r.Context())
		mu       sync.Mutex
		machines = b.Machines()
	)
	for _, m := range machines {
		m := m
		g.Go(func() error {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			var mstats []profileStat
			if err := m.Call(ctx, "Supervisor.Profiles", struct{}{}, &mstats); err != nil {
				log.Error.Printf("%q.\"Supervisor.Profiles\": %v", m.Addr, err)
				return nil
			}
			mu.Lock()
			stats[m.Addr] = mstats
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		profileErrorf(w, 500, "error fetching profiles: %v", err)
		return
	}

	aggregate := make(map[string]profileStat)
	for _, mstats := range stats {
		for _, p := range mstats {
			aggregate[p.Name] = profileStat{
				Name:  p.Name,
				Count: aggregate[p.Name].Count + p.Count,
			}
		}
	}

	all := make([]profileStat, 0, len(aggregate))
	for _, p := range aggregate {
		all = append(all, p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })
	err := indexTmpl.Execute(w, map[string]interface{}{
		"All":      all,
		"Machines": stats,
	})
	if err != nil {
		log.Error.Print(err)
	}
}

// Shutdown tears down resources associated with this B. It should be called
// by the driver to discard a session, usually in a defer:
//
//	b := bigmachine.Start()
//	defer b.Shutdown()
//	// driver code
func (b *B) Shutdown() {
	b.system.Shutdown()
}

// MaybeInit calls the method
//
//	Init(*B) error
//
// if it exists on iface.
func maybeInit(iface interface{}, b *B) error {
	type initer interface {
		Init(*B) error
	}
	init, ok := iface.(initer)
	if !ok {
		return nil
	}
	return init.Init(b)
}
