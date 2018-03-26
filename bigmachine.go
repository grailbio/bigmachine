// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"net/http"
	// Sha256 is imported because we use its implementation for
	// fingerprinting binaries.
	_ "crypto/sha256"
	"log"
	"os"
	"reflect"
	"sync"

	"github.com/grailbio/bigmachine/rpc"
	"golang.org/x/sync/errgroup"
)

// rpcPrefix is the path prefix used to serve RPC requests.
const rpcPrefix = "/bigrpc/"

// Impl is the System implementation that is used by bigmachine.
// Users can change this before bigmachine.Run is called, but use of
// driver package is encouraged instead.
var Impl System = localSystem{}

// Bigmachine maintains a set of global variables for service registration
// and state. It is done this way because bigmachine is intrinsically global:
// each process is a single bigmachine instance, and it must take over that
// process entirely.
var (
	supervisor *Service
	server     = rpc.NewServer()
	client     *rpc.Client

	stateMu  sync.Mutex
	services = make(map[string]*Service)
	machines = map[string]*Machine{}
	driver   bool
	running  bool
)

func init() {
	// TODO(marius): handle more profile types
	http.Handle("/debug/bigprof/profile", profileHandler("profile"))
	http.Handle("/debug/bigprof/heap", profileHandler("heap"))
	http.Handle(rpcPrefix, server)
}

// Run is the entry point for bigmachine. When Run is called by the
// driver process, it returns immediately; it never returns when
// called by machines.
//
// The driver should call the returned shutdown function if a clean
// shutdown is desired.
//
// Most users should use the driver package instead of calling Run
// directly.
func Run() (shutdown func()) {
	switch mode := os.Getenv("BIGMACHINE_MODE"); mode {
	case "":
		driver = true
	case "machine":
	default:
		log.Fatalf("invalid bigmachine mode %s", mode)
	}
	supervisor = Register(LocalSupervisor)
	if err := Impl.Init(); err != nil {
		log.Fatal(err)
	}
	var err error
	client, err = rpc.NewClient(Impl.HTTPClient(), rpcPrefix)
	if err != nil {
		log.Fatal(err)
	}
	stateMu.Lock()
	running = true
	stateMu.Unlock()
	if !driver {
		log.Fatal(Impl.Main())
	}
	return func() {}
}

// Driver tells whether this process is the driver process.
// It should only be called after Run.
func Driver() bool {
	return driver
}

// Server returns the bigmachine RPC server associated with this process.
func Server() *rpc.Server {
	return server
}

// Dial connects to the machine named by the provided address.
//
// The returned machine is not owned: it is not kept alive as Start
// does.
func Dial(ctx context.Context, addr string) (*Machine, error) {
	// TODO(marius): normalize addrs?
	// TODO(marius): collect machines from 'machines' as they become
	// unavailable and should be redialed. We should also embed some sort
	// of cookie/capability into the address so we can distinguish between
	// different instances of a machine on the same address.
	stateMu.Lock()
	m := machines[addr]
	if m == nil {
		m = &Machine{Addr: addr, owner: false}
		machines[addr] = m
		m.start()
	}
	stateMu.Unlock()
	return m, nil
}

// Start launches a new machine and returns it. The new machine may
// be in Starting state when it is returned. Start maintains a keepalive to
// the returned machine, thus tying the machine's lifetime with the caller
// process.
func Start(ctx context.Context) (*Machine, error) {
	m, err := Impl.Start(ctx)
	if err != nil {
		return nil, err
	}
	m.owner = true
	m.start()
	stateMu.Lock()
	machines[m.Addr] = m
	stateMu.Unlock()
	return m, nil
}

// StartN starts multiple machines simultaneously. See Start for
// details.
func StartN(ctx context.Context, n int) ([]*Machine, error) {
	g, ctx := errgroup.WithContext(ctx)
	machines := make([]*Machine, n)
	for i := range machines {
		i := i
		g.Go(func() error {
			var err error
			machines[i], err = Start(ctx)
			return err
		})
	}
	return machines, g.Wait()
}

// Register registers a new service and returns its handler. See
// package documentation for details about services, their lifetimes,
// and how they are dispatched.
//
// It is an error to register a service after Run has been called.
func Register(iface interface{}) *Service {
	stateMu.Lock()
	if running {
		stateMu.Unlock()
		panic("attempted to register a service while running")
	}

	// TODO: any way to not rely on order here?
	svc := &Service{
		instance: iface,
		name:     reflect.Indirect(reflect.ValueOf(iface)).Type().Name(),
	}
	for services[svc.name] != nil {
		svc.name = svc.name + "x"
	}
	services[svc.name] = svc
	stateMu.Unlock()
	if err := server.Register(svc.name, iface); err != nil {
		log.Fatal(err)
	}
	if err := maybeInit(iface, svc); err != nil {
		log.Fatal(err)
	}
	return svc
}

// A Service represents a service registered with bigmachine.
type Service struct {
	name     string
	instance interface{}
}

// Name returns the service's unique assigned name.
func (s *Service) Name() string {
	return s.name
}

// Local returns the local instance of a service.
func (s *Service) Local() interface{} {
	return s.instance
}

// MaybeInit calls the method
//
//	Init(*Service) error
//
// if it exists on iface.
func maybeInit(iface interface{}, svc *Service) error {
	type initer interface {
		Init(*Service) error
	}
	init, ok := iface.(initer)
	if !ok {
		return nil
	}
	return init.Init(svc)
}
