// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

/*
	Bigpi is an example bigmachine program that estimates digits of Pi
	using the Monte Carlo method. It distributes work by instantiating
	multiple machines and calling them to make samples, returning the
	total number of the samples that fell inside of the unit circle.

	We can run it locally with a small number of sample to test:

		% bigpi -n 1000000
		2018/03/16 15:21:05 waiting for machines to come online
		2018/03/16 15:21:08 machine http://localhost:63880/ RUNNING
		2018/03/16 15:21:08 machine http://localhost:63878/ RUNNING
		2018/03/16 15:21:08 machine http://localhost:63879/ RUNNING
		2018/03/16 15:21:08 machine http://localhost:63881/ RUNNING
		2018/03/16 15:21:08 machine http://localhost:63877/ RUNNING
		2018/03/16 15:21:08 all machines are ready
		2018/03/16 15:21:08 distributing work among 5 cores
		http://localhost:63878/: 2018/03/16 15:21:08 0/200000
		http://localhost:63880/: 2018/03/16 15:21:08 0/200000
		http://localhost:63879/: 2018/03/16 15:21:08 0/200000
		http://localhost:63881/: 2018/03/16 15:21:08 0/200000
		2018/03/16 15:21:08 total=784425 nsamples=1000000
		π = 3.1377

	By using a large EC2 instance we can distribute the work over 100s of cores
	trivially:

		% bigpi -bigsystem ec2 -bigec2type c5.18xlarge -n 1000000000000
		2018/03/20 21:00:05 waiting for machines to come online
		2018/03/20 21:01:09 machine https://ec2-54-213-185-145.us-west-2.compute.amazonaws.com:2000/ RUNNING
		2018/03/20 21:01:09 machine https://ec2-35-164-137-2.us-west-2.compute.amazonaws.com:2000/ RUNNING
		2018/03/20 21:01:09 machine https://ec2-34-208-105-231.us-west-2.compute.amazonaws.com:2000/ RUNNING
		2018/03/20 21:01:09 machine https://ec2-34-211-149-59.us-west-2.compute.amazonaws.com:2000/ RUNNING
		2018/03/20 21:01:09 machine https://ec2-34-223-251-92.us-west-2.compute.amazonaws.com:2000/ RUNNING
		2018/03/20 21:01:09 all machines are ready
		2018/03/20 21:01:09 distributing work among 360 cores
		https://ec2-34-208-105-231.us-west-2.compute.amazonaws.com:2000/: 2018/03/20 21:01:09 0/2777777777
		https://ec2-34-223-251-92.us-west-2.compute.amazonaws.com:2000/: 2018/03/20 21:01:09 0/2777777777
		...
		2018/03/20 21:13:27 total=785397678380 nsamples=1000000000000
		π = 3.141590713520

	Once a bigmachine program is running, we can profile it using the
	standard Go pprof tooling. The returned profile is sampled from the
	whole cluster and merged. In the first iteration of this program, this helped
	find a bug: we were using the global rand.Float64 which requires a lock.
	The CPU profile highlighted the lock contention easily:

		% go tool pprof localhost:3333/debug/bigprof/profile
		Fetching profile over HTTP from http://localhost:3333/debug/bigprof/profile
		Saved profile in /Users/marius/pprof/pprof.045821636.samples.cpu.001.pb.gz
		File: 045821636
		Type: cpu
		Time: Mar 16, 2018 at 3:17pm (PDT)
		Duration: 2.51mins, Total samples = 16.80mins (669.32%)
		Entering interactive mode (type "help" for commands, "o" for options)
		(pprof) top
		Showing nodes accounting for 779.47s, 77.31% of 1008.18s total
		Dropped 51 nodes (cum <= 5.04s)
		Showing top 10 nodes out of 58
		      flat  flat%   sum%        cum   cum%
		   333.11s 33.04% 33.04%    333.11s 33.04%  runtime.procyield
		   116.71s 11.58% 44.62%    469.55s 46.57%  runtime.lock
		    76.35s  7.57% 52.19%    347.21s 34.44%  sync.(*Mutex).Lock
		    65.79s  6.53% 58.72%     65.79s  6.53%  runtime.futex
		    41.48s  4.11% 62.83%    202.05s 20.04%  sync.(*Mutex).Unlock
		    34.10s  3.38% 66.21%    364.36s 36.14%  runtime.findrunnable
		       33s  3.27% 69.49%        33s  3.27%  runtime.cansemacquire
		    32.72s  3.25% 72.73%     51.01s  5.06%  runtime.runqgrab
		    24.88s  2.47% 75.20%     57.72s  5.73%  runtime.unlock
		    21.33s  2.12% 77.31%     21.33s  2.12%  math/rand.(*rngSource).Uint64

*/
package main

import (
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"net/http"
	"sync/atomic"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/driver"
	"golang.org/x/sync/errgroup"
)

func init() {
	gob.Register(circlePI{})
}

type circlePI struct{}

// Sample generates n points inside the unit square and reports
// how many of these fall inside the unit circle.
func (circlePI) Sample(ctx context.Context, n uint64, m *uint64) error {
	r := rand.New(rand.NewSource(rand.Int63()))
	for i := uint64(0); i < n; i++ {
		if i%1e7 == 0 {
			log.Printf("%d/%d", i, n)
		}
		x, y := r.Float64(), r.Float64()
		if (x-0.5)*(x-0.5)+(y-0.5)*(y-0.5) < 0.25 {
			*m++
		}
	}
	return nil
}

func main() {
	nsamples := flag.Int("n", 1e10, "number of samples to make")
	nmachine := flag.Int("nmach", 5, "number of machines to provision for the task")
	flag.Parse()
	b := driver.Start()
	defer b.Shutdown()

	// Launch a local web server so we have access to profiles.
	go func() {
		err := http.ListenAndServe(":3333", nil)
		log.Printf("http.ListenAndServe: %v", err)
	}()
	ctx := context.Background()

	// Start the desired number of machines.
	services := bigmachine.Services{
		"PI": circlePI{},
	}
	machines, err := b.StartN(ctx, *nmachine, services)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("waiting for machines to come online")
	for _, m := range machines {
		<-m.Wait(bigmachine.Running)
		log.Printf("machine %s %s", m.Addr, m.State())
		if err := m.Err(); err != nil {
			log.Fatal(err)
		}
		if mem, err := m.MemInfo(ctx); err != nil {
			log.Printf("meminfo %s: %v", m.Addr, err)
		} else {
			log.Printf("mem %s: %s", m.Addr, mem)
		}
		if disk, err := m.DiskInfo(ctx); err != nil {
			log.Printf("diskinfo %s: %v", m.Addr, err)
		} else {
			log.Printf("disk %s: %v", m.Addr, disk)
		}
	}
	log.Print("all machines are ready")
	// Number of samples per machine
	numPerMachine := uint64(*nsamples) / uint64(*nmachine)

	// Divide the total number of samples among all the processors on
	// each machine. Aggregate the counts and then report the estimate.
	var total uint64
	var cores int
	g, ctx := errgroup.WithContext(ctx)
	for _, m := range machines {
		m := m
		for i := 0; i < m.Maxprocs; i++ {
			cores++
			g.Go(func() error {
				var count uint64
				err := m.Call(ctx, "PI.Sample", numPerMachine/uint64(m.Maxprocs), &count)
				if err == nil {
					atomic.AddUint64(&total, count)
				}
				return err
			})
		}
	}
	log.Printf("distributing work among %d cores", cores)
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
	log.Printf("total=%d nsamples=%d", total, *nsamples)
	var (
		pi   = big.NewRat(int64(4*total), int64(*nsamples))
		prec = int(math.Log(float64(*nsamples)) / math.Log(10))
	)
	fmt.Printf("π = %s\n", pi.FloatString(prec))
}
