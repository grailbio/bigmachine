// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2system

import (
	"strings"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/config"
)

func init() {
	config.Register("bigmachine/ec2system", func(inst *config.Instance) {
		var system System

		// TODO(marius): maybe defer defaults to system impl?
		inst.BoolVar(&system.OnDemand, "ondemand", false, "use on-demand instances")
		inst.StringVar(&system.InstanceType, "instance", "m3.medium", "instance type to allocate")
		inst.StringVar(&system.AMI, "ami", "ami-4296ec3a", "AMI to bootstrap")
		inst.StringVar(&system.InstanceProfile, "instance-profile", "",
			"the instance profile with which to launch new instances")
		inst.StringVar(&system.SecurityGroup, "security-group", "",
			"the security group with which new instances are launched")
		diskspace := inst.Int("diskspace", 200, "the amount of (root) disk space to allocate")
		dataspace := inst.Int("dataspace", 0, "the amount of scratch/data space to allocate")
		inst.StringVar(&system.Binary, "binary",
			"http://grail-public-bin.s3-us-west-2.amazonaws.com/linux/amd64/ec2boot0.2",
			"the bootstrap bigmachine binary with which machines are launched")
		sshkeys := inst.String("sshkey", "", "comma-separated list of ssh keys to be installed")
		inst.StringVar(&system.Username, "username", "", "user name for tagging purposes")
		var sess *session.Session
		inst.InstanceVar(&sess, "aws", "aws", "AWS configuration for all EC2 calls")
		inst.Doc = "bigmachine/ec2system configures the default instance settings used for bigmachine's ec2 backend"
		inst.New = func() (interface{}, error) {
			system.Diskspace = uint(*diskspace)
			system.Dataspace = uint(*dataspace)
			system.SshKeys = strings.Split(*sshkeys, ",")
			system.AWSConfig = sess.Config
			return &system, nil
		}
	})
}
