// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"encoding/gob"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/grailbio/base/must"
)

// A System implements a set of methods to set up a bigmachine and
// start new machines. Systems are also responsible for providing an
// HTTP client that can be used to communicate between machines
// and drivers.
type System interface {
	// Name is the name of this system. It is used to multiplex multiple
	// system implementations, and thus should be unique among
	// systems.
	Name() string
	// Init is called when the bigmachine starts up in order to
	// initialize the system implementation. If an error is returned,
	// the Bigmachine fails.
	Init(*B) error
	// Main is called to start  a machine. The system is expected to
	// take over the process; the bigmachine fails if main returns (and
	// if it does, it should always return with an error).
	Main() error
	// HTTPClient returns an HTTP client that can be used to communicate
	// from drivers to machines as well as between machines.
	HTTPClient() *http.Client
	// ListenAndServe serves the provided handler on an HTTP server that
	// is reachable from other instances in the bigmachine cluster. If addr
	// is the empty string, the default cluster address is used.
	ListenAndServe(addr string, handle http.Handler) error
	// Start launches up to n new machines.  The returned machines can
	// be in Unstarted state, but should eventually become available.
	Start(ctx context.Context, n int) ([]*Machine, error)
	// Exit is called to terminate a machine with the provided exit code.
	Exit(int)
	// Shutdown is called on graceful driver exit. It's should be used to
	// perform system tear down. It is not guaranteed to be called.
	Shutdown()
	// Maxprocs returns the maximum number of processors per machine,
	// as configured. Returns 0 if is a dynamic value.
	Maxprocs() int
	// KeepaliveConfig returns the various keepalive timeouts that should
	// be used to maintain keepalives for machines started by this system.
	KeepaliveConfig() (period, timeout, rpcTimeout time.Duration)
	// Tail returns a reader that follows the bigmachine process logs.
	Tail(ctx context.Context, m *Machine) (io.Reader, error)
	// Read returns a reader that reads the contents of the provided filename
	// on the host. This is done outside of the supervisor to support external
	// monitoring of the host.
	Read(ctx context.Context, m *Machine, filename string) (io.Reader, error)
}

var (
	systemsMu sync.Mutex
	systems   = make(map[string]System)
)

// RegisterSystem is used by systems implementation to register a
// system implementation. RegisterSystem registers the implementation
// with gob, so that instances can be transmitted over the wire. It
// also registers the provided System instance as a default to use
// for the name to support bigmachine.Init.
func RegisterSystem(name string, system System) {
	gob.Register(system)
	systemsMu.Lock()
	defer systemsMu.Unlock()
	must.Nil(systems[name], "system ", name, " already registered")
	systems[name] = system
}

// Init initializes bigmachine. It should be called after flag
// parsing and global setup in bigmachine-based processes. Init is a
// no-op if the binary is not running as a bigmachine worker; if it
// is, Init never returns.
func Init() {
	name := os.Getenv("BIGMACHINE_SYSTEM")
	if name == "" {
		return
	}
	system, ok := systems[name]
	must.True(ok, "system ", name, " not found")
	must.Never("start returned: ", Start(system))
}
