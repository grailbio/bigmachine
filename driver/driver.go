// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package driver provides a convenient API for bigmachine drivers.
// It should be preferred over using the raw bigmachine APIs.
// Programs using the driver package should have the following form:
//
//	func main() {
//		flag.Parse()
//		// Other initialization
//		defer driver.Run()()
//		// Driver code
//	}
package driver

import (
	"flag"
	"log"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
)

var (
	systemFlag   = flag.String("bigsystem", "local", "system on which to run the bigmachine")
	instanceType = flag.String("bigec2type", "m3.medium", "instance type with which to launch a bigmachine EC2 cluster")
)

// Run starts a bigmachine as configured by the flags provided by
// this package. The returned shutdown function should be called when
// the driver exits in order to provide clean shutdown.
//
// Run will select a bigmachine system implementation and configure
// it according to the flags passed in. By default, it runs with a
// local implementation.
func Run() (shutdown func()) {
	switch *systemFlag {
	default:
		log.Fatalf("unrecognized system %s", *systemFlag)
	case "ec2":
		bigmachine.Impl = &ec2system.System{
			InstanceType: *instanceType,
		}
	case "local":
	}
	return bigmachine.Run()
}
