// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package ec2system implements a bigmachine System that launches
// machines on dedicated EC2 spot instances. Ec2machine bootstraps
// instances through the use of cloud config and a bootstrap binary
// that runs the bigmachine supervisor service. The new binaries are
// then uploaded via bigmachine's RPC mechanism and execed remotely.
//
// Ec2machine instances may be configured, but uses good defaults for
// GRAIL. It uses instance descriptions generated from Reflow's
// ec2instances tool to construct appropriate spot bid prices, and to
// configure instances according to their underlying characteristics.
// Ec2machine does not currently set up local storage beyond the boot
// gp2 EBS volume. (Its size may be configured.)
//
// Secure communications is set up through an ephemeral CA stored at
// /tmp/bigmachine.pem.
//
// TODO(marius): generalize this somewhere: grailmachine?
package ec2system

//go:generate go run ../../reflow/cmd/ec2instances/main.go instances

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"html/template"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system/instances"
	"golang.org/x/net/http2"
)

const (
	port                 = 2000
	maxConcurrentStreams = 2000
	authorityPath        = "/tmp/bigmachine.pem"

	// 214GiB is the smallest gp2 disk size that attains maximum
	// possible gp2 throughput.
	dataVolumeSliceSize = 214
)

var immortal = flag.Bool("ec2machineimmortal", false, "make immortal EC2 instances (debugging only)")

// TODO(marius): generate this from the the EC2 inventory JSON instead.
var instanceTypes map[string]instances.Type

func init() {
	instanceTypes = make(map[string]instances.Type)
	for _, typ := range instances.Types {
		instanceTypes[typ.Name] = typ
	}
}

// Instance is a default ec2machine System.
var Instance = new(System)

// System implements a bigmachine system for EC2 instances.
// See package docs for more details.
type System struct {
	// OnDemand determines whether to use on-demand or spot instances.
	OnDemand bool

	// InstanceType is the EC2 instance type to launch in this system.
	// It defaults to m3.medium.
	InstanceType string

	// AMI is the AMI used to boot instances with. The AMI must support
	// cloud config and use systemd. The default AMI is a recent stable CoreOS
	// build.
	AMI string

	// Region is the AWS region in which to launch the system's instances.
	// It defaults to us-west-2.
	Region string

	// InstanceProfile is the instance profile with which to launch the instance.
	// This should be set if the instances need AWS credentials.
	InstanceProfile string

	// SecurityGroup is the security group into which instances are launched.
	SecurityGroup string

	// Diskspace is the amount of disk space in GiB allocated
	// to the instance's root EBS volume. Its default is 200.
	Diskspace uint

	// Dataspace is the amount of data disk space allocated
	// in /mnt/data. It defaults to 0. Data are striped across
	// multiple gp2 EBS slices in order to improve throughput.
	Dataspace uint

	// Binary is the URL to a bootstrap binary to be used when launching
	// system instances. It should be a minimal bigmachine build that
	// contains the ec2machine implementation and runs bigmachine's
	// supervisor service. By default the following binary is used:
	//
	//	http://grail-bin.s3-us-west-2.amazonaws.com/linux/amd64/ec2boot0.1
	//
	// The binary is fetched by a vanilla curl(1) invocation, and thus needs
	// to be publicly available.
	Binary string

	// SshKeys is the list of sshkeys that installed as authorized keys
	// in the instance. On system initialization, SshKeys is amended
	// with the contents of $HOME/.ssh/id_rsa.pub, if it exists.
	SshKeys []string

	config instances.Type

	ec2 ec2iface.EC2API

	authority         *certificateAuthority
	authorityContents []byte
}

// Name returns the name of this system ("ec2").
func (s *System) Name() string { return "ec2" }

// Init initializes the system. Before validating the system
// configuration and providing defaults, Init checks that the
// architecture and OS reported by Go's runtime is amd64 and linux
// respectively. Currently these are the only supported architectures
// from which to launch ec2machine systems.
//
// Init also establishes the AWS API session with which it
// communicates to the EC2 API. It uses the default session
// constructor furnished by the AWS SDK.
func (s *System) Init(b *bigmachine.B) error {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		// TODO(marius): it would be nice to be able to provide companion
		// binaries that are of the correct architecture. (Or even build
		// them, e.g., if we have a Go package and a command like
		// "bigmachine run package...".) On the other hand, it's very nice
		// to have a single, static binary for this.
		return errors.E(errors.Precondition, "ec2machine is currently supported only on linux/amd64")
	}
	if s.InstanceType == "" {
		s.InstanceType = "m3.medium"
	}
	// TODO(marius): derive defaults from a config
	if s.AMI == "" {
		s.AMI = "ami-7c488704"
	}
	if s.Region == "" {
		s.Region = "us-west-2"
	}
	if s.InstanceProfile == "" {
		s.InstanceProfile = "arn:aws:iam::619867110810:instance-profile/bigmachine"
	}
	if s.SecurityGroup == "" {
		s.SecurityGroup = "sg-7390e50c"
	}
	if s.Diskspace == 0 {
		s.Diskspace = 200
	}
	if s.Binary == "" {
		s.Binary = "http://grail-bin.s3-us-west-2.amazonaws.com/linux/amd64/ec2boot0.1"
	}
	var ok bool
	s.config, ok = instanceTypes[s.InstanceType]
	if !ok {
		return fmt.Errorf("invalid instance type %q", s.InstanceType)
	}
	if s.config.Price[s.Region] == 0 {
		return fmt.Errorf("instance type %q not available in region %s", s.InstanceType, s.Region)
	}
	sshkeyPath := os.ExpandEnv("$HOME/.ssh/id_rsa.pub")
	sshKey, err := ioutil.ReadFile(sshkeyPath)
	if err == nil {
		s.SshKeys = append(s.SshKeys, string(sshKey))
	} else {
		log.Printf("failed to read ssh key from %s: %v; the user will not be able to ssh into the system", sshkeyPath, err)
	}
	sess, err := session.NewSession(&aws.Config{Region: aws.String(s.Region)})
	if err != nil {
		return err
	}
	s.ec2 = ec2.New(sess)
	s.authority, err = newCertificateAuthority(authorityPath)
	if err != nil {
		return err
	}
	s.authorityContents, err = ioutil.ReadFile(authorityPath)
	return err
}

// Start launches a new machine on the EC2 spot market. Start fails
// when no spot capacity is available for the requested instance
// type. After the instance is launched, Start asynchronously tags it
// with the bigmachine command line and binary, as well as other
// runtime information.
func (s *System) Start(ctx context.Context) (*bigmachine.Machine, error) {
	userData, err := s.cloudConfig().Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cloud-config: %v", err)
	}
	blockDevices := []*ec2.BlockDeviceMapping{
		{
			DeviceName: aws.String("/dev/xvda"),
			Ebs: &ec2.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int64(int64(50 + s.Diskspace)),
				VolumeType:          aws.String("gp2"),
			},
		},
	}
	nslice := s.numDataSlices()
	for i := 0; i < nslice; i++ {
		blockDevices = append(blockDevices, &ec2.BlockDeviceMapping{
			// Device names are ignored for NVMe devices; they may be
			// remapped into any NVMe name. Luckily, our devices are
			// all of uniform size, and so we just need to know how many
			// we have.
			DeviceName: aws.String(fmt.Sprintf("/dev/xvd%c", 'b'+i)),
			Ebs: &ec2.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int64(dataVolumeSliceSize),
				VolumeType:          aws.String("gp2"),
			},
		})
	}
	var instanceId string
	if s.OnDemand {
		params := &ec2.RunInstancesInput{
			ImageId:               aws.String(s.AMI),
			MaxCount:              aws.Int64(int64(1)),
			MinCount:              aws.Int64(int64(1)),
			BlockDeviceMappings:   blockDevices,
			DisableApiTermination: aws.Bool(false),
			DryRun:                aws.Bool(false),
			EbsOptimized:          aws.Bool(s.config.EBSOptimized),
			IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
				Arn: aws.String(s.InstanceProfile),
			},
			InstanceInitiatedShutdownBehavior: aws.String("terminate"),
			InstanceType:                      aws.String(s.config.Name),
			Monitoring: &ec2.RunInstancesMonitoringEnabled{
				Enabled: aws.Bool(true), // Required
			},
			UserData:         aws.String(base64.StdEncoding.EncodeToString(userData)),
			SecurityGroupIds: []*string{aws.String(s.SecurityGroup)},
		}
		resv, err := s.ec2.RunInstances(params)
		if err != nil {
			return nil, err
		}
		if n := len(resv.Instances); n != 1 {
			return nil, fmt.Errorf("expected 1 instance; got %d", n)
		}
		instanceId = *resv.Instances[0].InstanceId
	} else {
		// TODO(marius): should we use AvailabilityZoneGroup to ensure that
		// all instances land in the same AZ?
		params := &ec2.RequestSpotInstancesInput{
			ValidUntil: aws.Time(time.Now().Add(time.Minute)),
			SpotPrice:  aws.String(fmt.Sprintf("%.3f", s.config.Price[s.Region])),
			LaunchSpecification: &ec2.RequestSpotLaunchSpecification{
				ImageId:             aws.String(s.AMI),
				EbsOptimized:        aws.Bool(s.config.EBSOptimized),
				InstanceType:        aws.String(s.config.Name),
				BlockDeviceMappings: blockDevices,
				UserData:            aws.String(base64.StdEncoding.EncodeToString(userData)),
				IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
					Arn: aws.String(s.InstanceProfile),
				},
				SecurityGroupIds: []*string{aws.String(s.SecurityGroup)},
			},
		}
		resp, err := s.ec2.RequestSpotInstancesWithContext(ctx, params)
		if err != nil {
			return nil, err
		}
		if n := len(resp.SpotInstanceRequests); n != 1 {
			return nil, fmt.Errorf("ec2.RequestSpotInstances: got %d entries, want 1", n)
		}
		reqId := aws.StringValue(resp.SpotInstanceRequests[0].SpotInstanceRequestId)
		if reqId == "" {
			return nil, fmt.Errorf("ec2.RequestSpotInstances: empty request id")
		}
		if err := ec2WaitForSpotFulfillment(ctx, s.ec2, reqId); err != nil {
			return nil, fmt.Errorf("error while waiting for spot fulfillment: %v", err)
		}
		describe, err := s.ec2.DescribeSpotInstanceRequestsWithContext(ctx, &ec2.DescribeSpotInstanceRequestsInput{
			SpotInstanceRequestIds: []*string{aws.String(reqId)},
		})
		if err != nil {
			return nil, err
		}
		if n := len(describe.SpotInstanceRequests); n != 1 {
			return nil, fmt.Errorf("ec2.DescribeSpotInstanceRequests: got %d entries, want 1", n)
		}
		instanceId = aws.StringValue(describe.SpotInstanceRequests[0].InstanceId)
		if instanceId == "" {
			return nil, fmt.Errorf("ec2.DescribeSpotInstanceRequests: missing instance ID")
		}
	}
	// Asynhronously tag the instance so we don't hold up the process.
	go func() {
		// TODO(marius): there should be some abstraction that provides the name,
		// so that it can be overriden, etc. Also, having a user would be nice here.
		var (
			info   = bigmachine.LocalInfo()
			binary = filepath.Base(os.Args[0])
			tag    = fmt.Sprintf("%s(%s) %s (bigmachine)", binary, info.Digest.Short(), strings.Join(os.Args[1:], " "))
		)
		_, err := s.ec2.CreateTags(&ec2.CreateTagsInput{
			Resources: []*string{aws.String(instanceId)},
			Tags: []*ec2.Tag{
				{Key: aws.String("Name"), Value: aws.String(tag)},
				{Key: aws.String("GOARCH"), Value: aws.String(info.Goarch)},
				{Key: aws.String("GOOS"), Value: aws.String(info.Goos)},
				{Key: aws.String("Digest"), Value: aws.String(info.Digest.String())},
			},
		})
		if err != nil {
			log.Error.Printf("ec2.CreateTags: %v", err)
		}
	}()
	// TODO(marius): custom WaitUntilInstanceRunningWithContext that's more aggressive
	err = s.ec2.WaitUntilInstanceRunningWithContext(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(instanceId)},
	})
	if err != nil {
		return nil, err
	}
	describeInstance, err := s.ec2.DescribeInstances(&ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(instanceId)},
	})
	if err != nil {
		return nil, err
	}
	if len(describeInstance.Reservations) != 1 || len(describeInstance.Reservations[0].Instances) != 1 {
		return nil, fmt.Errorf("ec2.DescribeInstances %v: invalid output", instanceId)
	}
	instance := describeInstance.Reservations[0].Instances[0]
	if instance.PublicDnsName == nil || *instance.PublicDnsName == "" {
		return nil, fmt.Errorf("ec2.DescribeInstances %s: no public DNS name", instanceId)
	}
	m := new(bigmachine.Machine)
	m.Addr = fmt.Sprintf("https://%s:%d/", *instance.PublicDnsName, port)
	m.Maxprocs = int(s.config.VCPU)
	return m, nil
}

func (s *System) numDataSlices() int {
	return int((s.Dataspace + dataVolumeSliceSize - 1) / dataVolumeSliceSize)
}

// CloudConfig returns the cloudConfig instance as configured by the current system.
func (s *System) cloudConfig() *cloudConfig {
	c := new(cloudConfig)
	c.SshAuthorizedKeys = s.SshKeys
	// Turn off rebooting, updating, and locksmithd, all of which can
	// cause the instance to reboot. These are ephemeral instances and
	// we're not interested in these updates. (The AMI should be kept up to
	// date however.)
	c.CoreOS.Update.RebootStrategy = "off"
	c.AppendUnit(cloudUnit{Name: "update-engine.service", Command: "stop"})
	c.AppendUnit(cloudUnit{Name: "locksmithd.service", Command: "stop"})
	var dataDeviceName string
	switch nslice := s.numDataSlices(); nslice {
	case 0:
	case 1:
		// No need to set up striping in this case.
		dataDeviceName = "xvdb"
		if s.config.NVMe {
			dataDeviceName = "nvme1n1"
		}
		c.AppendUnit(cloudUnit{
			Name:    fmt.Sprintf("format-%s.service", dataDeviceName),
			Command: "start",
			Content: tmpl(`
			[Unit]
			Description=Format /dev/{{.name}}
			After=dev-{{.name}}.device
			Requires=dev-{{.name}}.device
			[Service]
			Type=oneshot
			RemainAfterExit=yes
			ExecStart=/usr/sbin/wipefs -f /dev/{{.name}}
			ExecStart=/usr/sbin/mkfs.ext4 -F /dev/{{.name}}
		`, args{"name": dataDeviceName}),
		})
	default:
		dataDeviceName = "md0"
		devices := make([]string, nslice)
		// Remap names for NVMe instances.
		for idx := range devices {
			if s.config.NVMe {
				devices[idx] = fmt.Sprintf("nvme%dn1", idx+1)
			} else {
				devices[idx] = fmt.Sprintf("xvd%c", 'b'+idx)
			}
		}
		c.AppendUnit(cloudUnit{
			Name:    fmt.Sprintf("format-%s.service", dataDeviceName),
			Command: "start",
			Content: tmpl(`
			[Unit]
			Description=Format /dev/{{.md}}
			After={{range $_, $name :=  .devices}}dev-{{$name}}.device {{end}}
			Requires={{range $_, $name := .devices}}dev-{{$name}}.device {{end}}
			[Service]
			Type=oneshot
			RemainAfterExit=yes
			ExecStart=/usr/sbin/mdadm --create --run --verbose /dev/{{.md}} --level=0 --chunk=256 --name=bigmachine --raid-devices={{.devices|len}} {{range $_, $name := .devices}}/dev/{{$name}} {{end}}
			ExecStart=/usr/sbin/mkfs.ext4 -F /dev/{{.md}}
		`, args{"devices": devices, "md": dataDeviceName}),
		})
	}
	if dataDeviceName != "" {
		c.AppendUnit(cloudUnit{
			Name:    "mnt-data.mount",
			Command: "start",
			Content: tmpl(`
			[Mount]
			What=/dev/{{.name}}
			Where=/mnt/data
			Type=ext4
			Options=data=writeback
		`, args{"name": dataDeviceName}),
		})
	}
	// Write the bootstrapping script. It fetches the binary and runs it.
	c.AppendFile(cloudFile{
		Permissions: "0755",
		Path:        "/opt/bin/bootmachine",
		Owner:       "root",
		Content: tmpl(`
		#!/bin/bash
		set -ex 
		bin=/opt/bin/ec2boot
		curl {{.binary}} >$bin
		chmod +x $bin
		export BIGMACHINE_MODE=machine
		export BIGMACHINE_SYSTEM=ec2
		export BIGMACHINE_ADDR=:{{.port}}
		exec $bin
		`, args{"binary": s.Binary, "port": port}),
	})
	c.AppendFile(cloudFile{
		Permissions: "0644",
		Path:        authorityPath,
		Content:     string(s.authorityContents),
	})
	// The bootmachine service runs the bootmachine script set up
	// previously. By default, the machine is shut down when the
	// bootmachine program terminates for any reason. This is the
	// mechanism of (automatic) instance termination.
	//
	// If we have a data disk, set TMPDIR to it.
	var environ string
	if dataDeviceName != "" {
		environ = "Environment=TMPDIR=/mnt/data"
	}
	c.AppendUnit(cloudUnit{
		Name:    "bootmachine.service",
		Enable:  true,
		Command: "start",
		Content: tmpl(`
			[Unit]
			Description=bootmachine
			Requires=network.target
			After=network.target
			{{if .mortal}}
			OnFailure=poweroff.target
			OnFailureJobMode=replace-irreversibly
			{{end}}
			[Service]
			Type=oneshot
			LimitNOFILE=infinity
			{{.environ}}
			ExecStart=/opt/bin/bootmachine
		`, args{"mortal": !*immortal, "environ": environ}),
	})
	return c
}

// HTTPClient returns an HTTP client configured to securely call
// instances launched by ec2machine over http/2.
func (s *System) HTTPClient() *http.Client {
	// TODO(marius): propagate error to caller
	config, _, err := s.authority.HTTPSConfig()
	if err != nil {
		// TODO: propagate error, or return error client
		log.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: config}
	http2.ConfigureTransport(transport)
	return &http.Client{Transport: transport}
}

// Main runs a bigmachine worker node. It sets up an HTTP server that
// performs mutual authentication with bigmachine clients launched
// from the same system instance. Main also starts a local HTTP
// server on port 3333 for debugging and local inspection.
func (s *System) Main() error {
	return http.ListenAndServe(":3333", nil)
}

// ListenAndServe serves the provided handler on a HTTP server
// configured for secure communications between ec2system
// instances.
func (s *System) ListenAndServe(addr string, handler http.Handler) error {
	if addr == "" {
		addr = os.Getenv("BIGMACHINE_ADDR")
	}
	if addr == "" {
		return errors.E(errors.Invalid, "no address defined")
	}
	_, config, err := s.authority.HTTPSConfig()
	if err != nil {
		return err
	}
	config.ClientAuth = tls.RequireAndVerifyClientCert
	server := &http.Server{
		TLSConfig: config,
		Addr:      addr,
		Handler:   handler,
	}
	http2.ConfigureServer(server, &http2.Server{
		MaxConcurrentStreams: maxConcurrentStreams,
	})
	return server.ListenAndServeTLS("", "")
}

// Exit terminates the process with the given exit code.
func (s *System) Exit(code int) {
	os.Exit(code)
}

// Shutdown is a no-op.
//
// TODO(marius): consider setting longer keepalives to maintain instances
// for future invocations.
func (s *System) Shutdown() {}

// Maxprocs returns the number of VCPUs in the system's configuration.
func (s *System) Maxprocs() int {
	return int(s.config.VCPU)
}

type args map[string]interface{}

// tmpl renders the template text, after first stripping common
// (whitespace) prefixes from text.
func tmpl(text string, args interface{}) string {
	lines := strings.Split(text, "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	var p int
	if len(lines) > 0 {
		p = strings.IndexFunc(lines[0], func(r rune) bool { return !unicode.IsSpace(r) })
		if p < 0 {
			p = 0
		}
	}
	for i, line := range lines {
		lines[i] = line[p:]
		if strings.TrimSpace(line[:p]) != "" {
			panic(fmt.Sprintf("nonspace prefix in %q", line))
		}
	}
	text = strings.Join(lines, "\n")
	t := template.Must(template.New("ec2template").Parse(text))
	var b bytes.Buffer
	if err := t.Execute(&b, args); err != nil {
		panic(err)
	}
	return b.String()
}
