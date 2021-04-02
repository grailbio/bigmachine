// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package monitor

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine"
	"golang.org/x/time/rate"
)

// T is the monitor type. It monitors instances started by the EC2 system.
type T struct {
	ec2     ec2iface.EC2API
	limiter *rate.Limiter
	ch      chan event
	cancel  func()
}

// machineInstance pairs a machine with its backing instance.
type machineInstance struct {
	machine    *bigmachine.Machine
	instanceID string
}

type machineInstanceSet map[machineInstance]struct{}

// event is the type for events processed by the monitoring loop.
type event interface{}

// eventStarted signals that a machine has been started by the EC2 system.
type eventStarted struct {
	instanceID string
	machine    *bigmachine.Machine
}

// eventStopped signals that a machine has been stopped.
type eventStopped struct {
	machine *bigmachine.Machine
}

// eventKeepaliveFailed signals that a keepalive call to a machine has failed.
type eventKeepaliveFailed struct {
	machine *bigmachine.Machine
}

// eventBatchDone indicates that processing of the current batch of machines is
// done.
type eventBatchDone struct{}

// Start starts a monitor.
func Start(ec2 ec2iface.EC2API, limiter *rate.Limiter) *T {
	m := &T{
		ec2:     ec2,
		limiter: limiter,
		ch:      make(chan event),
	}
	var ctx context.Context
	ctx, m.cancel = context.WithCancel(context.Background())
	go m.loop(ctx)
	return m
}

// Started notifies m of a started machine to be monitored.
func (m *T) Started(instanceID string, machine *bigmachine.Machine) {
	m.ch <- eventStarted{instanceID: instanceID, machine: machine}
}

// KeepaliveFailed notifies m of a failed call to Supervisor.Keepalive.
func (m *T) KeepaliveFailed(machine *bigmachine.Machine) {
	m.ch <- eventKeepaliveFailed{machine}
}

// Cancel cancels monitoring.
func (m *T) Cancel() {
	m.cancel()
}

func (m *T) loop(ctx context.Context) {
	var (
		machines      = make(map[*bigmachine.Machine]machineInstance)
		triggerCh     <-chan time.Time
		batch         = make(machineInstanceSet)
		batchInFlight machineInstanceSet
		maybeSchedule = func() {
			if len(batch) == 0 {
				return
			}
			if triggerCh == nil {
				res := m.limiter.Reserve()
				if !res.OK() {
					panic("limiter too restrictive")
				}
				triggerCh = time.After(res.Delay())
			}
		}
	)
	for {
		select {
		case e := <-m.ch:
			switch e := e.(type) {
			case eventStarted:
				go func() {
					select {
					case <-e.machine.Wait(bigmachine.Stopped):
					case <-ctx.Done():
						return
					}
					select {
					case m.ch <- eventStopped{e.machine}:
					case <-ctx.Done():
						return
					}
				}()
				machines[e.machine] = machineInstance{
					machine:    e.machine,
					instanceID: e.instanceID,
				}
			case eventStopped:
				delete(machines, e.machine)
			case eventKeepaliveFailed:
				mi, ok := machines[e.machine]
				if !ok {
					continue
				}
				if batchInFlight != nil {
					if _, ok := batchInFlight[mi]; ok {
						// We are already checking the machine, so do nothing
						// with this notification.
						continue
					}
					batch[mi] = struct{}{}
					continue
				}
				batch[mi] = struct{}{}
				maybeSchedule()
			case eventBatchDone:
				batchInFlight = nil
				maybeSchedule()
			}
		case <-triggerCh:
			triggerCh = nil
			batchInFlight = batch
			batch = make(machineInstanceSet)
			go func() {
				m.maybeCancel(ctx, batchInFlight)
				m.ch <- eventBatchDone{}
			}()
		case <-ctx.Done():
			return
		}
	}
}

// maybeCancel cancels a machine if its instance is in a terminal state. We use
// the instance status to recover faster than if we were to rely on the
// keepalive timeout alone.
func (m *T) maybeCancel(ctx context.Context, mis machineInstanceSet) {
	ids := make([]*string, 0, len(mis))
	byID := make(map[string]machineInstance, len(mis))
	for mi := range mis {
		ids = append(ids, aws.String(mi.instanceID))
		byID[mi.instanceID] = mi
	}
	input := &ec2.DescribeInstancesInput{InstanceIds: ids}
	output, err := m.ec2.DescribeInstancesWithContext(ctx, input)
	if err != nil {
		log.Printf("describing instances after keepalive failure: %v", err)
		return
	}
	for _, res := range output.Reservations {
		for _, inst := range res.Instances {
			stateName := aws.StringValue(inst.State.Name)
			switch stateName {
			case ec2.InstanceStateNameShuttingDown:
				fallthrough
			case ec2.InstanceStateNameTerminated:
				fallthrough
			case ec2.InstanceStateNameStopping:
				fallthrough
			case ec2.InstanceStateNameStopped:
				mi, ok := byID[aws.StringValue(inst.InstanceId)]
				if !ok {
					log.Printf("unexpected instance ID: %s", aws.StringValue(inst.InstanceId))
					continue
				}
				log.Printf("canceling %s because it is in state '%s'", mi.machine.Addr, stateName)
				mi.machine.Cancel()
			}
			if inst.StateReason != nil {
				// TODO(jcharumilind): Account spot failures and shift to
				// on-demand instances if there is too much contention in the
				// spot market.
				log.Printf(
					"instance %s: state:%s, code:%s, message:%s",
					aws.StringValue(inst.InstanceId),
					aws.StringValue(inst.State.Name),
					aws.StringValue(inst.StateReason.Code),
					aws.StringValue(inst.StateReason.Message),
				)
			}
		}
	}
}
