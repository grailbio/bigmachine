// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"net/http"
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
	Init() error
	// Main is called to start  a machine. The system is expected to
	// take over the process; the bigmachine fails if main returns (and
	// if it does, it should always return with an error).
	Main() error
	// HTTPClient returns an HTTP client that can be used to communicate
	// from drivers to machines as well as between machines.
	HTTPClient() *http.Client
	// Start launches a new machine. The returned machine can be in
	// Unstarted state, but should eventually become available.
	Start(context.Context) (*Machine, error)
	// Exit is called to terminate a machine with the provided exit code.
	Exit(int)
}
