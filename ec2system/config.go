// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2system

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/config"
)

// Defaults for the ec2boot binary. These are used when the "binary" value is empty.
// For backwards compatibility (old configs), any binary with the prefix
// defaultEc2BootPrefix is rewritten to the current version.
const (
	defaultEc2BootPrefix  = "https://grail-public-bin.s3-us-west-2.amazonaws.com/linux/amd64/ec2boot"
	defaultEc2BootVersion = "0.5"
	defaultEc2Boot        = defaultEc2BootPrefix + defaultEc2BootVersion
)

func init() {
	config.Register("bigmachine/ec2system", func(constr *config.Constructor) {
		var system System

		// TODO(marius): maybe defer defaults to system impl?
		constr.BoolVar(&system.OnDemand, "ondemand", false, "use on-demand instances")
		constr.StringVar(&system.InstanceType, "instance", "m3.medium", "instance type to allocate")
		// Flatcar-stable-2512.2.1-hvm
		constr.StringVar(&system.AMI, "ami", "ami-0bb54692374ac10a7", "AMI to bootstrap")
		constr.StringVar(&system.InstanceProfile, "instance-profile", "",
			"the instance profile with which to launch new instances")
		constr.StringVar(&system.SecurityGroup, "security-group", "",
			"the security group with which new instances are launched")
		constr.StringVar(&system.DefaultRegion, "default-region", "us-west-2", "default AWS region to use when one is not explicitly set via an aws.Config")
		diskspace := constr.Int("diskspace", 200, "the amount of (root) disk space to allocate")
		dataspace := constr.Int("dataspace", 0, "the amount of scratch/data space to allocate")
		constr.StringVar(&system.Binary, "binary",
			"",
			"the bootstrap bigmachine binary with which machines are launched")
		sshkeys := constr.String("sshkey", "", "comma-separated list of ssh keys to be installed")
		constr.InstanceVar(&system.Eventer, "eventer", "", "the event logger used to log bigmachine events")
		constr.StringVar(&system.Username, "username", "", "user name for tagging purposes")
		var sess *session.Session
		constr.InstanceVar(&sess, "aws", "aws", "AWS configuration for all EC2 calls")
		constr.Doc = "bigmachine/ec2system configures the default instances settings used for bigmachine's ec2 backend"
		constr.New = func() (interface{}, error) {
			system.Diskspace = uint(*diskspace)
			system.Dataspace = uint(*dataspace)
			system.SshKeys = strings.Split(*sshkeys, ",")
			system.AWSConfig = sess.Config
			return &system, nil
		}
	})
}
