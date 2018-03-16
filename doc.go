// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
	Package bigmachine implements a vertically integrated stack for
	distributed computing in Go. Go programs written with bigmachine are
	transparently distributed across a number of machines as
	instantiated by the backend used. (Currently supported: EC2, local
	machines.) Bigmachine clusters comprise a driver node and a number
	of bigmachine nodes (called "machines"). The driver node can create
	new machines and communicate with them; machines can call each
	other.

	Computing model

	On startup, a bigmachine program registers a number of services
	before calling driver.Run.

		import (
			"github.com/grailbio/bigmachine"
			"github.com/grailbio/bigmachine/driver"
			...
		)

		func main() {
			flag.Parse()
			service := bigmachine.Register(new(Service))
			shutdown := driver.Run()
			defer shutdown()
			// Driver code ...
		}

	When the program is run, driver.Run returns immediately: the
	program can then interact with the bigmachine package to create new
	machines and invoke methods on services on those machines.
	Bigmachine bootstraps machines by running the same binary, but in
	these runs, driver.Run never returns; instead it launches a server
	to handle calls from the driver program and other machines. Thus,
	services must be registered before driver.Run is called.

	A services's methods can be invoked so long as they are of the form:

		Func(ctx context.Context, arg argType, reply *replyType) error

	See package github.com/grailbio/bigmachine/rpc for more details.

	Machines are created by bigmachine.Start:

		m, err := bigmachine.Start(ctx)
		...

	Once a machine has started, we can call methods on its services:

		if err := m.Call(ctx, service, "MethodName", arg, &reply); err != nil {
			log.Print(err)
		} else {
			// Examine reply
		}

	A bigmachine program attempts to appear and act like a single program:

		- Each machine's standard output and error are copied to the driver;
		- bigmachine provides aggregating profile handlers at /debug/bigprof/
		  so that aggregate profiles may be taken over the full cluster;
		- command line flags are propagated from the driver to the machine,
		  so that a binary run can be configured in the usual way.

	The driver program maintains keepalives to all of its machines. Once
	this is no longer maintained  (e.g., because the driver finished, or
	crashed, or lost connectivity), the machines become idle and shut
	down.

	Services

	A service is any Go value that implements methods of the form given
	above. Services are instantiated by the user and registered with
	bigmachine. When a service is registered, bigmachine will also
	invoke an initialization method on the service if it exists.
	Per-machine initialization can be performed by this method.The form
	of the method is:

		Init(*Service) error

	If a non-nil error is returned, the machine is considered failed.
*/
package bigmachine
