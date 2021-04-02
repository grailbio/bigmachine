// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package monitor

import (
	"context"
	"encoding/gob"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/testsystem"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
	"golang.org/x/time/rate"
)

// TestCancellation verifies that machines backed by terminated instances are
// stopped by the monitor.
func TestCancellation(t *testing.T) {
	// We (somewhat strangely) attach our monitor to testsystem machines. It
	// does not make much sense to tie these machines to (fake) EC2 instances,
	// except that it allows us to conveniently test monitoring.
	const N = 100
	var (
		api     = NewAPI()
		sys     = testsystem.New()
		b       = bigmachine.Start(sys)
		limiter = rate.NewLimiter(rate.Inf, 1)
		m       = Start(api, limiter)
	)
	defer m.Cancel()
	machines, err := b.Start(context.Background(), N, bigmachine.Services{
		"Dummy": &dummyService{},
	})
	assert.Nil(t, err)
	for _, machine := range machines {
		<-machine.Wait(bigmachine.Running)
	}
	for i, machine := range machines {
		var sfx string
		switch {
		case i%4 == 0:
			sfx = "-terminated"
		case i%2 == 0:
			sfx = "-shutting-down"
		}
		m.Started(strconv.Itoa(i)+sfx, machine)
	}
	for _, machine := range machines {
		m.KeepaliveFailed(machine)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	for i, machine := range machines {
		if i%2 == 0 {
			// Expect that machines we tagged to be terminated above are
			// stopped.
			select {
			case <-machine.Wait(bigmachine.Stopped):
			case <-ctx.Done():
				t.Fatal("took too long")
			}
		} else {
			expect.EQ(t, machine.State(), bigmachine.Running)
		}
	}
	cancel()
}

// TestRateLimiting verifies that monitoring does not violate the rate limit
// imposed by the passed limiter.
func TestRateLimiting(t *testing.T) {
	const (
		Interval = 50 * time.Millisecond
		Nmachine = 100
	)
	var (
		api     = NewAPI()
		limiter = rate.NewLimiter(rate.Every(Interval), 1)
		m       = Start(api, limiter)
	)
	defer m.Cancel()
	machines := make([]*bigmachine.Machine, Nmachine)
	for i := range machines {
		machines[i] = &bigmachine.Machine{
			Addr: strconv.Itoa(i),
		}
		m.Started(strconv.Itoa(i), machines[i])
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for {
			time.Sleep(1 * time.Millisecond)
			j := rand.Intn(len(machines))
			m.KeepaliveFailed(machines[j])
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	time.Sleep(1 * time.Second)
	cancel()
	expect.LT(t, api.Rate(), float64(1*time.Second)/float64(Interval))
}

type fakeEC2API struct {
	ec2iface.EC2API
	start    time.Time
	reqCount int
}

func NewAPI() *fakeEC2API {
	return &fakeEC2API{
		start: time.Now(),
	}
}

// Rate returns the number of requests made per second up until the time at
// which it is called.
func (api *fakeEC2API) Rate() float64 {
	d := time.Since(api.start)
	return float64(api.reqCount) / float64(d.Seconds())
}

func (api *fakeEC2API) DescribeInstancesWithContext(
	ctx context.Context,
	input *ec2.DescribeInstancesInput,
	_ ...request.Option,
) (*ec2.DescribeInstancesOutput, error) {
	api.reqCount++
	defer time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	instances := make([]*ec2.Instance, len(input.InstanceIds))
	for i := range instances {
		code := int64(16) // running
		name := ec2.InstanceStateNameRunning
		switch {
		case strings.HasSuffix(aws.StringValue(input.InstanceIds[i]), "-terminated"):
			code = int64(48) // terminated
			name = ec2.InstanceStateNameTerminated
		case strings.HasSuffix(aws.StringValue(input.InstanceIds[i]), "-shutting-down"):
			code = int64(32) // shutting-down
			name = ec2.InstanceStateNameShuttingDown
		}
		instances[i] = &ec2.Instance{
			InstanceId: input.InstanceIds[i],
			State: &ec2.InstanceState{
				Code: aws.Int64(code),
				Name: aws.String(name),
			},
		}
	}
	return &ec2.DescribeInstancesOutput{
		Reservations: []*ec2.Reservation{
			{Instances: instances},
		},
	}, nil
}

type dummyService struct{}

func init() {
	gob.Register(&dummyService{})
}
