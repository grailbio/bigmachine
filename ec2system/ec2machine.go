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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/base64"
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/fatbin"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/ec2system/instances"
	"github.com/grailbio/bigmachine/internal/authority"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
	"golang.org/x/net/http2"
	"golang.org/x/time/rate"
)

const (
	maxConcurrentStreams = 20000
	authorityPath        = "/tmp/bigmachine.pem"

	// 334GiB is the smallest gp2 disk size that yields maximum throughput, as per
	// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
	minDataVolumeSliceSize = 335

	// The maximum number of EBS volumes an instance can have.
	// This is really ~40, but we're going to keep it to a-z to maintain
	// simple naming.
	maxInstanceDataVolumes = 25
)

func init() {
	bigmachine.RegisterSystem("ec2", new(System))
}

var (
	// RetryPolicy is used to retry failed EC2 API calls.
	retryPolicy = retry.Backoff(time.Second, 10*time.Second, 2)

	// Used to rate limit EC2 calls.
	limiter = rate.NewLimiter(rate.Limit(1), 2)
)

var immortal = flag.Bool("ec2machineimmortal", false, "make immortal EC2 instances (debugging only)")

// SetMortality conrols the mortality of EC2 instances for help with debugging
// low level boot time issues. It is equivalent to the 'ec2machineimmportal' flag
// for configurations where flags cannot be used.
func SetMortality(v bool) {
	*immortal = v
}

var (
	// InstanceTypes stores metadata for each known EC2 instance type.
	// TODO(marius): generate this from the the EC2 inventory JSON instead.
	instanceTypes map[string]instances.Type

	// imageInfo stores *ec2.Image values for all known AMIs;
	// describeImages coordinates retrieval of these values.
	imageInfo      sync.Map
	describeImages once.Map
)

func init() {
	instanceTypes = make(map[string]instances.Type)
	for _, typ := range instances.Types {
		instanceTypes[typ.Name] = typ
	}
}

// Instance is a default ec2machine System.
var Instance = new(System)

// A Flavor is the flavor of operating system used in the system.
// The system flavor adjusts for local variations in setup.
type Flavor int

const (
	// CoreOS indicates that the AMI is based on CoreOS.
	CoreOS Flavor = iota
	// Ubuntu indicates that the AMI is based on Ubuntu.
	Ubuntu
)

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

	// Flavor is the operating system flavor of the AMI.
	Flavor Flavor

	// AWSConfig is used to launch the system's instances.
	// Default region is us-west-2.
	AWSConfig *aws.Config

	// InstanceProfile is the instance profile with which to launch the instance.
	// This should be set if the instances need AWS credentials.
	InstanceProfile string

	// SecurityGroup is the security group into which instances are launched.
	SecurityGroup string

	// SecurityGroups are the security group into which instances are launched.
	// If set, it used in preference to SecurityGroup above.
	SecurityGroups []string

	// Subnet is the subnet into which instances are launched.
	Subnet string

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
	//	https://grail-public-bin.s3-us-west-2.amazonaws.com/linux/amd64/ec2boot0.3
	//
	// The binary is fetched by a vanilla curl(1) invocation, and thus needs
	// to be publicly available.
	Binary string

	// SshKeys is the list of sshkeys that installed as authorized keys
	// in the instance. On system initialization, SshKeys is amended
	// with the contents of $HOME/.ssh/id_rsa.pub, if it exists, and the
	// keys available in the SSH agent reachable by $SSH_AUTH_SOCK, if
	// one exists.
	SshKeys []string

	// The EC2 key pair name to associate with the created instances when
	// the instance this launched. This key name will appear in the EC2
	// instance's metadata.
	EC2KeyName string

	// The user running the application. For tagging.
	Username string

	// AdditionalFiles are added to the worker cloud-init configuration.
	AdditionalFiles []CloudFile
	// AdditionalUnits are added to the worker cloud-init configuration.
	AdditionalUnits []CloudUnit

	// AdditionalEC2Tags will be applied to this system's instances.
	AdditionalEC2Tags []*ec2.Tag

	privateKey *rsa.PrivateKey

	config instances.Type

	ec2 ec2iface.EC2API

	authority         *authority.T
	authorityContents []byte

	clientOnce   once.Task
	clientConfig *tls.Config
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
		self, err := fatbin.Self()
		if err != nil {
			return err
		}
		// TODO(marius): don't hardcode this as being linux/amd64
		rc, err := self.Open("linux", "amd64")
		if err == fatbin.ErrNoSuchImage {
			return errors.E(errors.Precondition, "binary has no linux/amd64 image; consider compiling with gofat or run on linux/amd64")
		} else if err != nil {
			return err
		}
		rc.Close()
	}
	if s.InstanceType == "" {
		s.InstanceType = "m3.medium"
	}
	// TODO(marius): derive defaults from a config
	if s.AMI == "" {
		s.AMI = "ami-4296ec3a"
	}
	if s.AWSConfig == nil {
		s.AWSConfig = &aws.Config{}
	}
	if s.AWSConfig.Region == nil {
		s.AWSConfig.Region = aws.String("us-west-2")
	}
	if s.Diskspace == 0 {
		s.Diskspace = 200
	}
	if s.Binary == "" {
		s.Binary = "https://grail-public-bin.s3-us-west-2.amazonaws.com/linux/amd64/ec2boot0.3"
	}
	var ok bool
	s.config, ok = instanceTypes[s.InstanceType]
	if !ok {
		return fmt.Errorf("invalid instance type %q", s.InstanceType)
	}
	if s.config.Price[*s.AWSConfig.Region] == 0 {
		return fmt.Errorf("instance type %q not available in region %s", s.InstanceType, *s.AWSConfig.Region)
	}

	// Generate a unique SSH key for this session. This is used for programmatic
	// SSH access to created machines.
	//
	// TODO(marius): can we add the private key to the user's ssh-agent?
	var err error
	s.privateKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	publicKey, err := ssh.NewPublicKey(&s.privateKey.PublicKey)
	if err != nil {
		return err
	}
	s.SshKeys = append(s.SshKeys, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(publicKey))))

	var userSshKeys []string
	sshkeyPath := os.ExpandEnv("$HOME/.ssh/id_rsa.pub")
	sshKey, err := ioutil.ReadFile(sshkeyPath)
	if err == nil {
		userSshKeys = append(userSshKeys, string(sshKey))
	}
	userSshKeys = append(userSshKeys, readSshAgentKeys()...)
	if b.IsDriver() && len(userSshKeys) == 0 {
		log.Printf("failed to read ssh key from %s: %v or from ssh agent; the user will not be able to ssh into the system", sshkeyPath, err)
	}
	s.SshKeys = append(s.SshKeys, userSshKeys...)

	sess, err := session.NewSession(s.AWSConfig)
	if err != nil {
		return err
	}
	s.ec2 = ec2.New(sess)
	s.authority, err = authority.New(authorityPath)
	if err != nil {
		return err
	}
	s.authorityContents, err = ioutil.ReadFile(authorityPath)
	if err != nil {
		return err
	}
	return err
}

func readSshAgentKeys() []string {
	agentSock := os.Getenv("SSH_AUTH_SOCK")
	if agentSock == "" {
		log.Debug.Print("no ssh agent found, skipping adding authorized keys")
		return nil
	}
	conn, err := net.Dial("unix", agentSock)
	if err != nil {
		log.Printf("error dialing ssh agent, skipping adding authorized keys: %v", err)
		return nil
	}
	defer conn.Close()
	ag := agent.NewClient(conn)
	keys, err := ag.List()
	if err != nil {
		log.Printf("error reading from ssh agent, skipping adding authorized keys: %v", err)
	}
	var keyStrings []string
	for _, key := range keys {
		keyStrings = append(keyStrings, string(ssh.MarshalAuthorizedKey(key)))
	}
	return keyStrings
}

// Start launches a new machine on the EC2 spot market. Start fails
// when no spot capacity is available for the requested instance
// type. After the instance is launched, Start asynchronously tags it
// with the bigmachine command line and binary, as well as other
// runtime information.
func (s *System) Start(ctx context.Context, count int) ([]*bigmachine.Machine, error) {
	userData, err := s.cloudConfig().Marshal()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cloud-config: %v", err)
	}
	err = describeImages.Do(s.AMI, func() error {
		out, err := s.ec2.DescribeImagesWithContext(ctx, &ec2.DescribeImagesInput{
			ImageIds: []*string{aws.String(s.AMI)},
		})
		if err != nil {
			return err
		}
		if len(out.Images) != 1 || aws.StringValue(out.Images[0].ImageId) != s.AMI {
			return errors.New("image not found")
		}
		imageInfo.Store(s.AMI, out.Images[0])
		return nil
	})
	if err != nil {
		if err == ctx.Err() {
			describeImages.Forget(s.AMI)
		}
		return nil, errors.E("describe-images", s.AMI, err)
	}
	infoIface, ok := imageInfo.Load(s.AMI)
	if !ok {
		panic(s.AMI)
	}
	info := infoIface.(*ec2.Image)
	rootDeviceName := info.RootDeviceName
	if rootDeviceName == nil {
		rootDeviceName = aws.String("/dev/xvda")
	}
	blockDevices := []*ec2.BlockDeviceMapping{
		{
			DeviceName: rootDeviceName,
			Ebs: &ec2.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int64(int64(50 + s.Diskspace)),
				VolumeType:          aws.String("gp2"),
			},
		},
	}
	nslice, sliceSize := s.sliceConfig()
	for i := 0; i < nslice; i++ {
		blockDevices = append(blockDevices, &ec2.BlockDeviceMapping{
			// Device names are ignored for NVMe devices; they may be
			// remapped into any NVMe name. Luckily, our devices are
			// all of uniform size, and so we just need to know how many
			// we have.
			DeviceName: aws.String(fmt.Sprintf("/dev/xvd%c", 'b'+i)),
			Ebs: &ec2.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int64(sliceSize),
				VolumeType:          aws.String("gp2"),
			},
		})
	}
	var run func() ([]string, error)
	securityGroups := []*string{aws.String(s.SecurityGroup)}
	if len(s.SecurityGroups) > 0 {
		securityGroups = make([]*string, len(s.SecurityGroups))
		for i := range s.SecurityGroups {
			securityGroups[i] = aws.String(s.SecurityGroups[i])
		}
	}

	var ec2KeyName *string
	if len(s.EC2KeyName) > 0 {
		ec2KeyName = aws.String(s.EC2KeyName)
	}

	if s.OnDemand {
		run = func() ([]string, error) {
			resv, err := s.ec2.RunInstances(&ec2.RunInstancesInput{
				SubnetId:              aws.String(s.Subnet),
				ImageId:               aws.String(s.AMI),
				MaxCount:              aws.Int64(int64(count)),
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
				SecurityGroupIds: securityGroups,
				KeyName:          ec2KeyName,
			})
			if err != nil {
				return nil, err
			}
			if len(resv.Instances) == 0 {
				return nil, errors.E(errors.Invalid, "expected at least 1 instance")
			}
			ids := make([]string, len(resv.Instances))
			for i := range ids {
				ids[i] = *resv.Instances[i].InstanceId
			}
			return ids, nil
		}
	} else {
		// TODO(marius): should we use AvailabilityZoneGroup to ensure that
		// all instances land in the same AZ?
		run = func() ([]string, error) {
			resp, err := s.ec2.RequestSpotInstancesWithContext(ctx, &ec2.RequestSpotInstancesInput{
				ValidUntil:    aws.Time(time.Now().Add(time.Minute)),
				SpotPrice:     aws.String(fmt.Sprintf("%.3f", s.config.Price[*s.AWSConfig.Region])),
				InstanceCount: aws.Int64(int64(count)),
				LaunchSpecification: &ec2.RequestSpotLaunchSpecification{
					SubnetId:            aws.String(s.Subnet),
					ImageId:             aws.String(s.AMI),
					EbsOptimized:        aws.Bool(s.config.EBSOptimized),
					InstanceType:        aws.String(s.config.Name),
					BlockDeviceMappings: blockDevices,
					UserData:            aws.String(base64.StdEncoding.EncodeToString(userData)),
					IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
						Arn: aws.String(s.InstanceProfile),
					},
					SecurityGroupIds: securityGroups,
					KeyName:          ec2KeyName,
				},
			})
			if err != nil {
				return nil, err
			}
			if len(resp.SpotInstanceRequests) == 0 {
				return nil, errors.E(errors.Invalid, "ec2.RequestSpotInstances: got 0 entries")
			}
			n := len(resp.SpotInstanceRequests)
			describeInput := &ec2.DescribeSpotInstanceRequestsInput{
				SpotInstanceRequestIds: make([]*string, n),
			}
			for i := range describeInput.SpotInstanceRequestIds {
				describeInput.SpotInstanceRequestIds[i] = resp.SpotInstanceRequests[i].SpotInstanceRequestId
			}
			if err := s.ec2.WaitUntilSpotInstanceRequestFulfilledWithContext(ctx, describeInput); err != nil {
				return nil, err
			}
			describe, err := s.ec2.DescribeSpotInstanceRequestsWithContext(ctx, describeInput)
			if err != nil {
				return nil, err
			}
			if got, want := n, len(describeInput.SpotInstanceRequestIds); got != want {
				return nil, fmt.Errorf("ec2.DescribeSpotInstanceRequests: got %d entries, want %d", got, want)
			}
			instanceIds := make([]string, n)
			for i := range instanceIds {
				instanceIds[i] = aws.StringValue(describe.SpotInstanceRequests[i].InstanceId)
			}
			return instanceIds, nil
		}
	}

	// TODO(marius): use fine-grained error handling in the case of spot instances.
	// TODO(marius): we can also avoid common cases of RequestLimitExceeded by pushing
	// instance count into this API.
	var instanceIds []string
	for retries := 0; err == nil; retries++ {
		// We apply a rate limit here to avoid thundering herds of multiple
		// machine requests, perfectly synchronized. This could have been
		// solved by adding jitter to the retry policy as well, but a rate limiter
		// is somewhat easier to reason about, and corresponds with the
		// policies used to limit requests to the EC2 API.
		if err := limiter.Wait(ctx); err != nil {
			break
		}
		instanceIds, err = run()
		if err == nil {
			break
		}
		if aerr, ok := err.(awserr.Error); !ok || aerr.Code() != "RequestLimitExceeded" {
			break
		}
		log.Error.Printf("ec2machine: retrying request limit error: %v", err)
		err = retry.Wait(ctx, retryPolicy, retries)
	}
	if err != nil {
		return nil, err
	}
	instanceIdsp := make([]*string, len(instanceIds))
	for i := range instanceIdsp {
		instanceIdsp[i] = aws.String(instanceIds[i])
	}

	// Asynhronously tag the instance so we don't hold up the process.
	go func() {
		// TODO(marius): there should be some abstraction that provides the name,
		// so that it can be overriden, etc. Also, having a user would be nice here.
		var (
			info   = bigmachine.LocalInfo()
			binary = filepath.Base(os.Args[0])
			tag    = fmt.Sprintf("%s:%s(%s) %s (bigmachine)", s.Username, binary, info.Digest.Short(), strings.Join(os.Args[1:], " "))
		)
		if len(tag) > 250 { // EC2 tags are limited to 255 characters.
			tag = tag[:250] + "..."
		}
		_, err := s.ec2.CreateTags(&ec2.CreateTagsInput{
			Resources: instanceIdsp,
			Tags: append([]*ec2.Tag{
				{Key: aws.String("Name"), Value: aws.String(tag)},
				{Key: aws.String("GOARCH"), Value: aws.String(info.Goarch)},
				{Key: aws.String("GOOS"), Value: aws.String(info.Goos)},
				{Key: aws.String("Digest"), Value: aws.String(info.Digest.String())},
				{Key: aws.String("bigmachine"), Value: aws.String("true")},
				{Key: aws.String("bigmachine:binary"), Value: aws.String(binary)},
			}, s.AdditionalEC2Tags...),
		})
		if err != nil {
			log.Error.Printf("ec2.CreateTags: %v", err)
		}
	}()
	// TODO(marius): custom WaitUntilInstanceRunningWithContext that's more aggressive
	describeInput := &ec2.DescribeInstancesInput{
		InstanceIds: instanceIdsp,
	}
	if err := s.ec2.WaitUntilInstanceRunningWithContext(ctx, describeInput); err != nil {
		log.Error.Printf("WaitUntilInstanceRunning: %v", err)
		describeInstance, err := s.ec2.DescribeInstancesWithContext(ctx, describeInput)
		if err != nil {
			return nil, err
		}
		for _, reserv := range describeInstance.Reservations {
			for _, inst := range reserv.Instances {
				log.Printf("instance %s: %s", aws.StringValue(inst.InstanceId), inst.State)
			}
		}

		return nil, err
	}
	describeInstance, err := s.ec2.DescribeInstancesWithContext(ctx, describeInput)
	if err != nil {
		return nil, err
	}
	if len(describeInstance.Reservations) != 1 || len(describeInstance.Reservations[0].Instances) != len(instanceIds) {
		return nil, errors.E(errors.Invalid, fmt.Sprintf("ec2.DescribeInstances: invalid output: %+v", describeInstance))
	}
	machines := make([]*bigmachine.Machine, len(instanceIds))
	for i, instance := range describeInstance.Reservations[0].Instances {
		addr := getAddress(instance)
		if len(addr) == 0 {
			return nil, fmt.Errorf("ec2.DescribeInstances %s[%d]: no dns name or ip addresss available", aws.StringValue(instance.InstanceId), i)
		}
		machines[i] = new(bigmachine.Machine)
		machines[i].Addr = fmt.Sprintf("https://%s/", addr)
		machines[i].Maxprocs = int(s.config.VCPU)
	}
	return machines, nil
}

func getAddress(instance *ec2.Instance) string {
	for _, ptr := range []*string{
		instance.PublicDnsName,
		instance.PublicIpAddress,
		instance.PrivateIpAddress,
		// NOTE: do not return a private DNS name since in it is not guaranteed to
		//       be resolvable externally.
	} {
		if val := aws.StringValue(ptr); len(val) > 0 {
			return val
		}
	}
	return ""
}

func (s *System) sliceConfig() (nslice int, sliceSize int64) {
	sliceSize = minDataVolumeSliceSize
	nslice = int((int64(s.Dataspace) + sliceSize - 1) / sliceSize)
	if nslice <= maxInstanceDataVolumes {
		return
	}
	nslice = maxInstanceDataVolumes
	sliceSize = int64(s.Dataspace) / int64(nslice)
	return
}

// CloudConfig returns the cloudConfig instance as configured by the current system.
func (s *System) cloudConfig() *cloudConfig {
	c := new(cloudConfig)
	c.SshAuthorizedKeys = s.SshKeys
	c.Flavor = s.Flavor

	if s.Flavor == CoreOS {
		// Turn off rebooting, updating, and locksmithd, all of which can
		// cause the instance to reboot. These are ephemeral instances and
		// we're not interested in these updates. (The AMI should be kept up to
		// date however.)
		c.CoreOS.Update.RebootStrategy = "off"
		c.AppendUnit(CloudUnit{Name: "update-engine.service", Command: "stop"})
		c.AppendUnit(CloudUnit{Name: "locksmithd.service", Command: "stop"})
	}

	var (
		// DataDeviceName is the device (possibly striped) that should be used
		// for the data partition.
		dataDeviceName string
		// Devices is all the devices used to compose storage.
		devices []string
	)
	switch nslice, _ := s.sliceConfig(); nslice {
	case 0:
	case 1:
		// No need to set up striping in this case.
		dataDeviceName = "xvdb"
		if s.config.NVMe {
			dataDeviceName = "nvme1n1"
		}
		c.AppendUnit(CloudUnit{
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
			ExecStart=/usr/bin/env wipefs -f /dev/{{.name}}
			ExecStart=/usr/bin/env mkfs.ext4 -F /dev/{{.name}}
		`, args{"name": dataDeviceName}),
		})
	default:
		dataDeviceName = "md0"
		devices = make([]string, nslice)
		// Remap names for NVMe instances.
		for idx := range devices {
			if s.config.NVMe {
				devices[idx] = fmt.Sprintf("nvme%dn1", idx+1)
			} else {
				devices[idx] = fmt.Sprintf("xvd%c", 'b'+idx)
			}
		}
		c.AppendUnit(CloudUnit{
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
			ExecStart=/usr/bin/env mdadm --create --run --verbose /dev/{{.md}} --level=0 --chunk=256 --name=bigmachine --raid-devices={{.devices|len}} {{range $_, $name := .devices}}/dev/{{$name}} {{end}}
			ExecStart=/usr/bin/env mkfs.ext4 -F /dev/{{.md}}
		`, args{"devices": devices, "md": dataDeviceName}),
		})
	}
	if dataDeviceName != "" {
		c.AppendUnit(CloudUnit{
			Name:    "mnt-data.mount",
			Command: "start",
			Content: tmpl(`
			[Unit]
			After=format-{{.name}}.service
			Requires=format-{{.name}}.service
			[Mount]
			What=/dev/{{.name}}
			Where=/mnt/data
			Type=ext4
			Options=data=writeback
		`, args{"name": dataDeviceName}),
		})

		// In this case we have to explicitly remove devices from the
		// cloud-init config so that these are not added automatically
		// added to the fstab by cloud-init. A single-argument mount
		// spec disables the mount.
		if s.Flavor == Ubuntu {
			c.AppendMount([]string{dataDeviceName})
			for _, dev := range devices {
				c.AppendMount([]string{dev})
			}
		}
	}

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
	// Increase the open-file limit. The reduce shuffle opens many
	// filedescriptors.
	const nropen = 32 << 20    // per-process limit
	const filemax = nropen * 4 // system-wide limit
	c.AppendFile(CloudFile{
		Permissions: "0644",
		Path:        "/etc/sysctl.d/90-bigmachine",
		Owner:       "root",
		Content: tmpl(`
    fs.file-max = {{.filemax}}
    fs.nr_open = {{.nropen}}

		`, args{"filemax": filemax, "nropen": nropen}),
	})

	for _, f := range s.AdditionalFiles {
		c.AppendFile(f)
	}
	for _, u := range s.AdditionalUnits {
		c.AppendUnit(u)
	}

	// Write the bootstrapping script. It fetches the binary and runs it.
	c.AppendFile(CloudFile{
		Permissions: "0755",
		Path:        "/opt/bin/bootmachine",
		Owner:       "root",
		Content: tmpl(`
		#!/bin/bash
		set -e
		bin=/tmp/ec2boot
		curl -s {{.binary}} >$bin
		chmod +x $bin
		export BIGMACHINE_MODE=machine
		export BIGMACHINE_SYSTEM=ec2
		export BIGMACHINE_ADDR=:{{443}}
		$bin -log=debug || true
		sleep 30
		exit 1
		`, args{"binary": s.Binary}),
	})
	c.AppendFile(CloudFile{
		Permissions: "0644",
		Path:        authorityPath,
		Content:     string(s.authorityContents),
	})

	sysctlPath := "/lib/systemd/systemd-sysctl"
	if s.Flavor == CoreOS {
		sysctlPath = "/usr/lib/systemd/systemd-sysctl"
	}
	c.AppendUnit(CloudUnit{
		Name:    "update-sysctl.service",
		Enable:  true,
		Command: "start",
		Content: tmpl(`
			[Unit]
			Description=Update sysctl
			[Service]
			ExecStart={{.sysctlPath}} /etc/sysctl.d/90-bigmachine
		`, args{"sysctlPath": sysctlPath}),
	})

	// Note: LimitNOFILE must be set explicitly, instead of just "infinity", since
	// the "inifinity" will expand the process'es current hard limit, which is
	// typically 1M.
	//
	// We also adjust bootmachine's OOM score so that it will be killed over
	// things like the ssh processes that might monitor the kernel message
	// buffers.
	c.AppendUnit(CloudUnit{
		Name:    "bootmachine.service",
		Enable:  true,
		Command: "start",
		Content: tmpl(`
			[Unit]
			Description=bootmachine
			Requires=network.target
			After=network.target
			{{if .data}}
			After=mnt-data.mount
			Requires=mnt-data.mount
			{{end}}
			{{if .mortal}}
			OnFailure=poweroff.target
			OnFailureJobMode=replace-irreversibly
			{{end}}
			[Service]
			OOMScoreAdjust=1000
			Type=oneshot
			LimitNOFILE={{.nropen}}
			{{.environ}}
			ExecStart=/opt/bin/bootmachine
		`, args{"mortal": !*immortal, "environ": environ, "nropen": nropen, "data": dataDeviceName != ""}),
	})
	return c
}

const httpTimeout = 30 * time.Second

// HTTPClient returns an HTTP client configured to securely call
// instances launched by ec2machine over http/2.
func (s *System) HTTPClient() *http.Client {
	// TODO(marius): propagate error to caller
	err := s.clientOnce.Do(func() (err error) {
		s.clientConfig, _, err = s.authority.HTTPSConfig()
		return
	})
	if err != nil {
		// TODO: propagate error, or return error client
		log.Fatal(err)
	}
	transport := &http.Transport{
		Dial:                (&net.Dialer{Timeout: httpTimeout}).Dial,
		TLSClientConfig:     s.clientConfig,
		TLSHandshakeTimeout: httpTimeout,
	}
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

func (s *System) Tail(ctx context.Context, m *bigmachine.Machine) (io.Reader, error) {
	u, err := url.Parse(m.Addr)
	if err != nil {
		return nil, err
	}
	return s.run(ctx, u.Hostname(), "sudo journalctl --output=cat -f -u bootmachine"), nil
}

func (s *System) Read(ctx context.Context, m *bigmachine.Machine, filename string) (io.Reader, error) {
	u, err := url.Parse(m.Addr)
	if err != nil {
		return nil, err
	}
	return s.run(ctx, u.Hostname(), "cat "+filename), nil
}

func (s *System) dialSSH(addr string) (*ssh.Client, error) {
	signer, err := ssh.NewSignerFromKey(s.privateKey)
	if err != nil {
		return nil, err
	}
	config := &ssh.ClientConfig{
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         5 * time.Second,
	}
	switch s.Flavor {
	case CoreOS:
		config.User = "core"
	case Ubuntu:
		config.User = "ubuntu"
	default:
		config.User = "root"
	}
	return ssh.Dial("tcp", addr+":22", config)
}

var sshRetryPolicy = retry.Backoff(time.Second, 10*time.Second, 1.5)

// run runs the provided command on the remote machine named by the provided
// address; the returned io.Reader is a byte stream of the command's combined
// output (standard output and standard error). run may retry the command multiple
// times; the returned reader is the concatenated output of all tries.
func (s *System) run(ctx context.Context, addr string, command string) io.Reader {
	r, w := io.Pipe()
	go func() {
		var err error
		for retries := 0; ; retries++ {
			err = s.runSSH(addr, w, command)
			if err == nil {
				break
			}
			log.Error.Printf("tail %v: %v", addr, err)
			if strings.HasPrefix(err.Error(), "ssh: unable to authenticate") {
				break
			}
			if _, ok := err.(*ssh.ExitError); ok {
				break
			}
			if err = retry.Wait(ctx, sshRetryPolicy, retries); err != nil {
				break
			}
		}
		w.CloseWithError(err)
	}()
	return r
}

func (s *System) runSSH(addr string, w io.Writer, command string) error {
	conn, err := s.dialSSH(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	sess, err := conn.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	sess.Stdout = w
	sess.Stderr = w
	return sess.Run(command)
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

func (*System) KeepaliveConfig() (period, timeout, rpcTimeout time.Duration) {
	period = time.Minute
	timeout = 10 * time.Minute
	rpcTimeout = 2 * time.Minute
	return
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
