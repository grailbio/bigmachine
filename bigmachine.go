// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"expvar"
	"html/template"
	"net/http"
	"sort"
	"strings"
	"time"
	// Sha256 is imported because we use its implementation for
	// fingerprinting binaries.
	_ "crypto/sha256"
	"os"
	"sync"

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

	server *rpc.Server
	client *rpc.Client

	mu       sync.Mutex
	machines map[string]*Machine
	driver   bool
	running  bool
}

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
func Start(system System) *B {
	b := &B{
		system:   system,
		machines: make(map[string]*Machine),
	}
	b.run()
	// Test systems run in a single process space and thus
	// expvar would panic with duplicate key errors.
	if system.Name() != "testsystem" {
		expvar.Publish("machines", &machineVars{b})
	}
	return b
}

// System returns this B's System implementation.
func (b *B) System() System { return b.system }

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
	b.client, err = rpc.NewClient(b.system.HTTPClient(), RpcPrefix)
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
	supervisor := StartSupervisor(b, b.system, b.server, true)
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
	// TODO(marius): collect machines from 'machines' as they become
	// unavailable and should be redialed. We should also embed some sort
	// of cookie/capability into the address so we can distinguish between
	// different instances of a machine on the same address.
	b.mu.Lock()
	m := b.machines[addr]
	if m == nil {
		m = &Machine{Addr: addr, owner: false}
		b.machines[addr] = m
		m.start(b)
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

// HandleDebug registers diagnostic http endpoints on the provided
// ServeMux.
func (b *B) HandleDebug(mux *http.ServeMux) {
	mux.Handle("/debug/bigmachine/pprof/profile", &profileHandler{b, "profile"})
	mux.Handle("/debug/bigmachine/pprof/heap", &profileHandler{b, "heap"})
	mux.HandleFunc("/debug/bigmachine/pprof/", b.pprofIndex)
	mux.Handle("/debug/bigmachine/status", &statusHandler{b})
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
