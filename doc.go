// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
	Package bigmachine implements a vertically integrated stack for
	distributed computing in Go. Go programs written with bigmachine are
	transparently distributed across a number of machines as
	instantiated by the backend used. (Currently supported: EC2, local
	machines, unit tests.) Bigmachine clusters comprise a driver node
	and a number of bigmachine nodes (called "machines"). The driver
	node can create new machines and communicate with them; machines can
	call each other.

	Computing model

	On startup, a bigmachine program calls driver.Start. Driver.Start
	configures a bigmachine instance based on a set of standard flags
	and then starts it. (Users desiring a lower-level API can use
	bigmachine.Start directly.)

		import (
			"github.com/grailbio/bigmachine"
			"github.com/grailbio/bigmachine/driver"
			...
		)

		func main() {
			flag.Parse()
			// Additional configuration and setup.
			b, shutdown := driver.Start()
			defer shutdown()

			// Driver code...
		}

	When the program is run, driver.Start returns immediately: the program
	can then interact with the returned bigmachine B to create new
	machines, define services on those machines, and invoke methods on
	those services. Bigmachine bootstraps machines by running the same
	binary, but in these runs, driver.Start never returns; instead it
	launches a server to handle calls from the driver program and other
	machines.

	A machine is started by (*B).Start. Machines must be configured with
	at least one service:

		m, err := b.Start(ctx, bigmachine.Services{
			"MyService": &MyService{Param1: value1, ...},
		})

	Users may then invoke methods on the services provided by the
	returned machine. A services's methods can be invoked so long as
	they are of the form:

		Func(ctx context.Context, arg argType, reply *replyType) error

	See package github.com/grailbio/bigmachine/rpc for more details.

	Methods are named by the sevice and method name, separated by a dot
	('.'), e.g.: "MyService.MyMethod":

		if err := m.Call(ctx, "MyService.MyMethod", arg, &reply); err != nil {
			log.Print(err)
		} else {
			// Examine reply
		}

	Since service instances must be serialized so that they can be transmitted
	to the remote machine, and because we do not know the service types
	a priori, any type that can appear as a service must be registered with
	gob. This is usually done in an init function in the package that declares
	type:

		type MyService struct { ... }

		func init() {
			// MyService has method receivers
			gob.Register(new(MyService))
		}


	Vertical computing

	A bigmachine program attempts to appear and act like a single program:

		- Each machine's standard output and error are copied to the driver;
		- bigmachine provides aggregating profile handlers at /debug/bigmachine/pprof
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
