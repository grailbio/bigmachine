// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Command ec2boot is a minimal bigmachine binary that is intended
// for bootstrapping binaries on EC2. It is used by ec2machine in
// this way.
package main

import (
	"log"

	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system"
)

func main() {
	bigmachine.Impl = ec2system.Instance
	bigmachine.Run()
	log.Fatal("bigmachine.Main returned")
}
