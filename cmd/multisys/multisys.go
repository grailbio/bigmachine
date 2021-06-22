// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// multisys is an example bigmachine program that uses multiple computing
// clusters to independently estimate π. It illustrates bigmachine's ability to
// use multiple bigmachine.System instances to configure and manage multiple
// computing clusters.
//
// The clusters estimate π using the Monte Carlo method (see bigpi). Each
// cluster collects sample points for the same duration, by default 20s.
// Clusters with more computing power, by way of more or faster machines, will
// consider more sample points.
//
// Clusters are configured as (system, numMachine) pairs. By default, multisys
// uses two clusters using local systems: one with 2 machines and one with 6
// machines.
//
//  % multisys
//  I0611 22:31:49.682976   45649 multisys.go:226] bigmachine/local:2: status at http://127.0.0.1:3333/bigmachine/0/status
//  I0611 22:31:49.682979   45649 multisys.go:226] bigmachine/local:6: status at http://127.0.0.1:3333/bigmachine/1/status
//  I0611 22:31:49.774646   45649 multisys.go:234] bigmachine/local:2: waiting for machines to come online
//  I0611 22:31:49.775678   45649 multisys.go:234] bigmachine/local:6: waiting for machines to come online
//  I0611 22:31:51.919868   45649 multisys.go:237] bigmachine/local:6: machine https://localhost:33139/ RUNNING
//  I0611 22:31:52.958645   45649 multisys.go:237] bigmachine/local:6: machine https://localhost:44725/ RUNNING
//  I0611 22:31:52.958669   45649 multisys.go:237] bigmachine/local:6: machine https://localhost:37463/ RUNNING
//  I0611 22:31:53.005860   45649 multisys.go:237] bigmachine/local:6: machine https://localhost:40629/ RUNNING
//  I0611 22:31:53.005878   45649 multisys.go:237] bigmachine/local:6: machine https://localhost:38117/ RUNNING
//  I0611 22:31:53.005887   45649 multisys.go:237] bigmachine/local:6: machine https://localhost:37861/ RUNNING
//  I0611 22:31:53.005895   45649 multisys.go:242] bigmachine/local:6: all machines are ready
//  I0611 22:31:53.005906   45649 multisys.go:267] bigmachine/local:6: distributing work among 6 cores
//  I0611 22:31:53.574701   45649 multisys.go:237] bigmachine/local:2: machine https://localhost:46535/ RUNNING
//  I0611 22:31:53.574725   45649 multisys.go:237] bigmachine/local:2: machine https://localhost:42687/ RUNNING
//  I0611 22:31:53.574738   45649 multisys.go:242] bigmachine/local:2: all machines are ready
//  I0611 22:31:53.574759   45649 multisys.go:267] bigmachine/local:2: distributing work among 2 cores
//  I0611 22:31:59.024124   45649 local.go:116] machine https://localhost:37861/ terminated
//  I0611 22:31:59.024363   45649 local.go:116] machine https://localhost:40629/ terminated
//  I0611 22:31:59.024363   45649 local.go:116] machine https://localhost:33139/ terminated
//  I0611 22:31:59.024561   45649 local.go:116] machine https://localhost:38117/ terminated
//  I0611 22:31:59.024564   45649 local.go:116] machine https://localhost:37463/ terminated
//  I0611 22:31:59.024710   45649 local.go:116] machine https://localhost:44725/ terminated
//  I0611 22:31:59.583062   45649 local.go:116] machine https://localhost:46535/ terminated
//  I0611 22:31:59.583075   45649 local.go:116] machine https://localhost:42687/ terminated
//                system    machines       samples     π estimate
//      bigmachine/local           2     599000000     3.14169444
//      bigmachine/local           6    1799000000    3.141569261
//
// The default is equivalent to running:
//
//  % multisys -cluster=bigmachine/local:2 -cluster=bigmachine/local:6
//
// Other clusters can be configured with systems available in the configuration
// profile, loaded from $HOME/grail/profile per grail.Init.
//
//   % multisys -cluster=bigmachine/local:3 -cluster=bigmachine/ec2system:2
//   I0611 23:40:50.576393   54940 multisys.go:225] bigmachine/ec2system:2: status at http://127.0.0.1:3333/bigmachine/1/status
//   I0611 23:40:50.576402   54940 multisys.go:225] bigmachine/local:3: status at http://127.0.0.1:3333/bigmachine/0/status
//   I0611 23:40:50.850481   54940 multisys.go:233] bigmachine/local:3: waiting for machines to come online
//   I0611 23:40:54.136284   54940 multisys.go:236] bigmachine/local:3: machine https://localhost:39387/ RUNNING
//   I0611 23:40:54.319191   54940 multisys.go:236] bigmachine/local:3: machine https://localhost:41691/ RUNNING
//   I0611 23:40:54.319213   54940 multisys.go:236] bigmachine/local:3: machine https://localhost:45851/ RUNNING
//   I0611 23:40:54.319220   54940 multisys.go:241] bigmachine/local:3: all machines are ready
//   I0611 23:40:54.319232   54940 multisys.go:266] bigmachine/local:3: distributing work among 3 cores
//   I0611 23:41:00.326318   54940 local.go:116] machine https://localhost:41691/ terminated
//   I0611 23:41:00.326343   54940 local.go:116] machine https://localhost:39387/ terminated
//   I0611 23:41:00.326537   54940 local.go:116] machine https://localhost:45851/ terminated
//   I0611 23:41:07.401206   54940 multisys.go:233] bigmachine/ec2system:2: waiting for machines to come online
//   I0611 23:42:19.270927   54940 multisys.go:236] bigmachine/ec2system:2: machine https://ec2-34-215-227-27.us-west-2.compute.amazonaws.com/i-0fa4f9fe0f57aafeb/ RUNNING
//   I0611 23:42:19.309629   54940 multisys.go:236] bigmachine/ec2system:2: machine https://ec2-18-237-149-184.us-west-2.compute.amazonaws.com/i-0a8e3d6f8a7d1d3ac/ RUNNING
//   I0611 23:42:19.309644   54940 multisys.go:241] bigmachine/ec2system:2: all machines are ready
//   I0611 23:42:19.309661   54940 multisys.go:266] bigmachine/ec2system:2: distributing work among 4 cores
//                     system    machines      samples    π estimate
//           bigmachine/local           3    905000000    3.14153730
//       bigmachine/ec2system           2    359000000    3.14150672
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
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigmachine"
	_ "github.com/grailbio/bigmachine/ec2system"
	"golang.org/x/sync/errgroup"
)

func init() {
	gob.Register(circlePI{})
}

type clusterSpec struct {
	// instance is the profile instance name of the bigmachine.System used to
	// provide machines for the cluster.
	instance string
	// nMachine is the number of machines in the cluster.
	nMachine int
}

func (s clusterSpec) String() string {
	return fmt.Sprintf("%s:%d", s.instance, s.nMachine)
}

// clusterSpecsValue implements the flag.Value interface and is used to
// configure the different systems/clusters to use to compute π.
type clusterSpecsValue struct {
	specs []clusterSpec
}

func (_ clusterSpecsValue) String() string {
	return ""
}

func (v *clusterSpecsValue) Set(s string) error {
	split := strings.Split(s, ":")
	if len(split) != 2 {
		return fmt.Errorf(
			"cluster must be 'system-profile-instance:num-machines', "+
				"e.g. 'bigmachine/ec2system:3': %s", s)
	}
	nMachine, err := strconv.Atoi(split[1])
	if err != nil {
		return fmt.Errorf("num-machines '%s' must be integer: %v", split[1], err)
	}
	v.specs = append(v.specs, clusterSpec{instance: split[0], nMachine: nMachine})
	return nil
}

// defaultClusterSpecs defines the clusters to use by default.
var defaultClusterSpecs = []clusterSpec{
	{"bigmachine/local", 2},
	{"bigmachine/local", 6},
}

type circlePI struct{}

// sampleResult holds the result of a sampling. Its fields are exported so that
// they can be gob-encoded.
type sampleResult struct {
	// Total is the total number of samples collected.
	Total uint64
	// In is the number of samples that fell in the unit circle.
	In uint64
}

// Sample generates points inside the unit square for duration d and reports
// how many of these fall inside the unit circle.
func (circlePI) Sample(ctx context.Context, d time.Duration, result *sampleResult) error {
	end := time.Now().Add(d)
	r := rand.New(rand.NewSource(rand.Int63()))
	for time.Now().Before(end) && ctx.Err() == nil {
		for i := 0; i < 1e6; i++ {
			result.Total++
			x, y := r.Float64(), r.Float64()
			if (x-0.5)*(x-0.5)+(y-0.5)*(y-0.5) <= 0.25 {
				result.In++
			}
		}
	}
	return ctx.Err()
}

func main() {
	var specsValue clusterSpecsValue
	flag.Var(
		&specsValue,
		"cluster",
		"cluster spec as 'profile-instance:num-machines', e.g. 'bigmachine/ec2system:3'",
	)
	duration := flag.Duration(
		"duration",
		20*time.Second,
		"the duration for which clusters will sample points",
	)
	grail.Init()
	bigmachine.Init()
	specs := specsValue.specs
	if len(specs) == 0 {
		specs = defaultClusterSpecs
	}
	var (
		ctx     = context.Background()
		wg      sync.WaitGroup
		results = make([]sampleResult, len(specs))
	)
	for i, spec := range specs {
		i := i
		spec := spec
		wg.Add(1)
		go func() {
			results[i] = findPI(ctx, i, spec, *duration)
			wg.Done()
		}()
	}
	// Launch a local web server so we have access to profiles.
	go func() {
		errServe := http.ListenAndServe(":3333", nil)
		log.Printf("http.ListenAndServe: %v", errServe)
	}()
	wg.Wait()
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 4, ' ', tabwriter.AlignRight)
	_, err := fmt.Fprintln(tw, "system\tmachines\tsamples\tπ estimate\t")
	must.Nil(err)
	for i, r := range results {
		var (
			pi   = big.NewRat(int64(4*r.In), int64(r.Total))
			prec = int(math.Log(float64(r.Total)) / math.Log(10))
		)
		_, err = fmt.Fprintf(tw, "%s\t%d\t%d\t%s\t\n",
			specs[i].instance, specs[i].nMachine, r.Total, pi.FloatString(prec))
		must.Nil(err)
	}
	must.Nil(tw.Flush())
}

func findPI(ctx context.Context, idx int, spec clusterSpec, d time.Duration) sampleResult {
	var system bigmachine.System
	config.Must(spec.instance, &system)
	b := bigmachine.Start(system)
	defer func() {
		b.Shutdown()
		// TODO(jcharumilind): Remove this hack once shutdown is synchronous.
		time.Sleep(2 * time.Second)
	}()
	pfx := fmt.Sprintf("/debug/bigmachine/%d/", idx)
	b.HandleDebugPrefix(pfx, http.DefaultServeMux)
	log.Printf("%s: status at http://127.0.0.1:3333/debug/bigmachine/%d/status", spec, idx)
	// Start the desired number of machines, each with the circlePI service.
	machines, err := b.Start(ctx, spec.nMachine, bigmachine.Services{
		"PI": circlePI{},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("%s: waiting for machines to come online", spec)
	for _, m := range machines {
		<-m.Wait(bigmachine.Running)
		log.Printf("%s: machine %s %s", spec, m.Addr, m.State())
		if err := m.Err(); err != nil {
			log.Fatal(err)
		}
	}
	log.Printf("%s: all machines are ready", spec)

	// Divide the total number of samples among all the processors on each
	// machine. Aggregate the counts and then report the estimate.
	var (
		total uint64
		in    uint64
		cores int
	)
	g, ctx := errgroup.WithContext(ctx)
	for _, m := range machines {
		m := m
		for i := 0; i < m.Maxprocs; i++ {
			cores++
			g.Go(func() error {
				var result sampleResult
				err := m.Call(ctx, "PI.Sample", d, &result)
				if err == nil {
					atomic.AddUint64(&total, result.Total)
					atomic.AddUint64(&in, result.In)
				}
				return err
			})
		}
	}
	log.Printf("%s: distributing work among %d cores", spec, cores)
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
	return sampleResult{Total: total, In: in}
}
