# Bigmachine

Bigmachine is a toolkit for building self-managing serverless applications
in [Go](https://golang.org/).
Bigmachine provides an API that lets a driver process
form an ad-hoc cluster of machines to
which user code is transparently distributed.

User code is exposed through services,
which are stateful Go objects associated with each machine.
Services expose one or more Go methods that may
be dispatched remotely.
User services can call remote user services;
the driver process may also make service calls.

Programs built using Bigmachine are agnostic
to the underlying machine implementation,
allowing distributed systems to be easily tested
through an [in-process implementation](https://godoc.org/github.com/grailbio/bigmachine/testsystem),
or inspected during development using [local Unix processes](https://godoc.org/github.com/grailbio/bigmachine#Local).

Bigmachine currently supports instantiating clusters of
[EC2 machines](https://godoc.org/github.com/grailbio/bigmachine/ec2system);
other systems may be implemented with a [relatively compact Go interface](https://godoc.org/github.com/grailbio/bigmachine#System).

- API documentation: [godoc.org/github.com/grailbio/bigmachine](https://godoc.org/github.com/grailbio/bigmachine)
- Issue tracker: [github.com/grailbio/bigmachine/issues](https://github.com/grailbio/bigmachine/issues)
- [![CI](https://github.com/grailbio/bigmachine/workflows/CI/badge.svg)](https://github.com/grailbio/bigmachine/actions?query=workflow%3ACI)
- Implementation notes: [github.com/grailbio/bigmachine/blob/master/docs/impl.md](https://github.com/grailbio/bigmachine/blob/master/docs/impl.md)

Help wanted!
- [GCP compute engine backend](https://github.com/grailbio/bigmachine/issues/1)
- [Azure VM backend](https://github.com/grailbio/bigmachine/issues/2)

# A walkthrough of a simple Bigmachine program

Command [bigpi](https://github.com/grailbio/bigmachine/blob/master/cmd/bigpi/bigpi.go)
is a relatively silly use of cluster computing,
but illustrative nonetheless.
Bigpi estimates the value of $\pi$
by sampling $N$ random coordinates inside of the unit square,
counting how many $C \le N$ fall inside of the unit circle.
Our estimate is then $\pi = 4*C/N$.

This is inherently parallelizable:
we can generate samples across a large number of nodes,
and then when we're done,
they can be summed up to produce our estimate of $\pi$.

To do this in Bigmachine,
we first define a service that samples some $n$ points
and reports how many fell inside the unit circle.

```
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
```

The only notable aspect of this code is the signature of `Sample`,
which follows the schema below:
methods that follow this convention may be dispatched remotely by Bigmachine,
as we shall see soon.

```
func (service) Name(ctx context.Context, arg argtype, reply *replytype) error
```

Next follows the program's `func main`.
First, we do the regular kind of setup a main might:
define some flags,
parse them,
set up logging.
Afterwards, a driver must call
[`driver.Start`](https://godoc.org/github.com/grailbio/bigmachine/driver#Start),
which initializes Bigmachine
and sets up the process so that it may be bootstrapped properly on remote nodes.
([Package driver](https://godoc.org/github.com/grailbio/bigmachine/driver)
provides high-level facilities for configuring and bootstrapping Bigmachine;
adventurous users may use the lower-level facilitied in
[package bigmachine](https://godoc.org/github.com/grailbio/bigmachine)
to accomplish the same.)
`driver.Start()` returns a [`*bigmachine.B`](https://godoc.org/gitub.com/grailbio/bigmachine#B)
which can be used to start new machines.

```
func main() {
	var (
		nsamples = flag.Int("n", 1e10, "number of samples to make")
		nmachine = flag.Int("nmach", 5, "number of machines to provision for the task")
	)
	log.AddFlags()
	flag.Parse()
	b := driver.Start()
	defer b.Shutdown()
```

Next,
we start a number of machines (as configured by flag nmach),
wait for them to finish launching,
and then distribute our sampling among them,
using a simple "scatter-gather" RPC pattern.
First, let's look at the code that starts the machines
and waits for them to be ready.

```
// Start the desired number of machines,
// each with the circlePI service.
machines, err := b.Start(ctx, *nmachine, bigmachine.Services{
	"PI": circlePI{},
})
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
}
log.Print("all machines are ready")
```

Machines are started with [`(*B).Start`](https://godoc.org/github.com/grailbio/bigmachine#B.Start),
to which we provide the set of services that should be installed on each machine.
(The service object provided is serialized and initialized on the remote machine,
so it may include any desired parameters.)
Start returns a slice of
[`Machine`](https://godoc.org/github.com/grailbio/bigmachine#Machine)
instances representing each machine that was launched.
Machines can be in a number of
[states](https://godoc.org/github.com/grailbio/bigmachine#State).
In this case,
we keep it simple and just wait for them to enter their running states,
after which the underlying machines are fully bootstrapped and the services
have been installed and initialized.
At this point,
all of the machines are ready to receive RPC calls.

The remainder of `main` distributes a portion of
the total samples to be taken to each machine,
waits for them to complete,
and then prints with the precision warranted by the number of samples taken.
Note that this code further subdivides the work by calling PI.Sample
once for each processor available on the underlying machines
as defined by [`Machine.Maxprocs`](https://godoc.org/github.com/grailbio/bigmachine#Machine.Maxprocs),
which depends on the physical machine configuration.


```
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
```

We can now build and run our binary like an ordinary Go binary.

```
$ go build
$ ./bigpi
2019/10/01 16:31:20 waiting for machines to come online
2019/10/01 16:31:24 machine https://localhost:42409/ RUNNING
2019/10/01 16:31:24 machine https://localhost:44187/ RUNNING
2019/10/01 16:31:24 machine https://localhost:41618/ RUNNING
2019/10/01 16:31:24 machine https://localhost:41134/ RUNNING
2019/10/01 16:31:24 machine https://localhost:34078/ RUNNING
2019/10/01 16:31:24 all machines are ready
2019/10/01 16:31:24 distributing work among 5 cores
2019/10/01 16:32:05 total=7853881995 nsamples=10000000000
π = 3.1415527980
```

Here,
Bigmachine distributed computation across logical machines,
each corresponding to a single core on the host system.
Each machine ran in its own Unix process (with its own address space),
and RPC happened through mutually authenticated HTTP/2 connections.

[Package driver](https://godoc.org/github.com/grailbio/bigmachine/driver)
provides some convenient flags that helps configure the Bigmachine runtime.
Using these, we can configure Bigmachine to launch machines into EC2 instead:

```
$ ./bigpi -bigm.system=ec2
2019/10/01 16:38:10 waiting for machines to come online
2019/10/01 16:38:43 machine https://ec2-54-244-211-104.us-west-2.compute.amazonaws.com/ RUNNING
2019/10/01 16:38:43 machine https://ec2-54-189-82-173.us-west-2.compute.amazonaws.com/ RUNNING
2019/10/01 16:38:43 machine https://ec2-34-221-143-119.us-west-2.compute.amazonaws.com/ RUNNING
...
2019/10/01 16:38:43 all machines are ready
2019/10/01 16:38:43 distributing work among 5 cores
2019/10/01 16:40:19 total=7853881995 nsamples=10000000000
π = 3.1415527980
```

Once the program is running,
we can use standard Go tooling to examine its behavior.
For example,
[expvars](https://golang.org/pkg/expvar/)
are aggregated across all of the machines managed by Bigmachine,
and the various profiles (CPU, memory, contention, etc.)
are available as merged profiles through `/debug/bigmachine/pprof`.
For example,
in the first version of `bigpi`,
the CPU profile highlighted a problem:
we were using the global `rand.Float64` which requires a lock;
the resulting contention was easily identifiable through the CPU profile:

```
$ go tool pprof localhost:3333/debug/bigmachine/pprof/profile
Fetching profile over HTTP from http://localhost:3333/debug/bigmachine/pprof/profile
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
```

And after the fix,
it looks much healthier:

```
$ go tool pprof localhost:3333/debug/bigmachine/pprof/profile
...
      flat  flat%   sum%        cum   cum%
    29.09s 35.29% 35.29%     82.43s   100%  main.circlePI.Sample
    22.95s 27.84% 63.12%     52.16s 63.27%  math/rand.(*Rand).Float64
    16.09s 19.52% 82.64%     16.09s 19.52%  math/rand.(*rngSource).Uint64
     9.05s 10.98% 93.62%     25.14s 30.49%  math/rand.(*rngSource).Int63
     4.07s  4.94% 98.56%     29.21s 35.43%  math/rand.(*Rand).Int63
     1.17s  1.42%   100%      1.17s  1.42%  math/rand.New
         0     0%   100%     82.43s   100%  github.com/grailbio/bigmachine/rpc.(*Server).ServeHTTP
         0     0%   100%     82.43s   100%  github.com/grailbio/bigmachine/rpc.(*Server).ServeHTTP.func2
         0     0%   100%     82.43s   100%  golang.org/x/net/http2.(*serverConn).runHandler
         0     0%   100%     82.43s   100%  net/http.(*ServeMux).ServeHTTP
```

# GOOS, GOARCH, and Bigmachine

When using Bigmachine's
[EC2 machine implementation](https://godoc.org/github.com/grailbio/bigmachine/ec2system),
the process is bootstrapped onto remote EC2 instances.
Currently,
the only supported GOOS/GOARCH combination for these are linux/amd64.
Because of this,
the driver program must also be linux/amd64.
However,
Bigmachine also understands the
[fatbin format](https://godoc.org/github.com/grailbio/base/fatbin),
so that users can compile fat binaries using the gofat tool.
For example,
the above can be run on a macOS driver if the binary is built using gofat instead of 'go':

```
macOS $ GO111MODULE=on go get github.com/grailbio/base/cmd/gofat
go: finding github.com/grailbio/base/cmd/gofat latest
go: finding github.com/grailbio/base/cmd latest
macOS $ gofat build
macOS $ ./bigpi -bigm.system=ec2
...
```
