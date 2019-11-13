// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"crypto"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/rpc"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

const (
	maxTeeBuffer     = 1 << 20
	memProfilePeriod = time.Minute
)

var (
	digester     = digest.Digester(crypto.SHA256)
	binaryDigest digest.Digest
	digestOnce   sync.Once
)

func binary() (io.ReadCloser, error) {
	// TODO(marius): use /proc/self/exe on Linux
	path, err := os.Executable()
	if err != nil {
		return nil, err
	}
	return os.Open(path)
}

// Supervisor is the system service installed on every machine.
type Supervisor struct {
	b       *B
	system  System
	server  *rpc.Server
	nextc   chan time.Time
	healthy uint32

	mu sync.Mutex
	// binaryPath contains the path of the last
	// binary uploaded in preparation for Exec.
	binaryPath string
	environ    []string
}

// StartSupervisor starts a new supervisor based on the provided arguments.
func StartSupervisor(ctx context.Context, b *B, system System, server *rpc.Server) *Supervisor {
	s := &Supervisor{
		b:      b,
		system: system,
		server: server,
	}
	s.healthy = 1
	s.nextc = make(chan time.Time)
	go s.watchdog(ctx)
	return s
}

// Info contains system information about a machine.
type Info struct {
	// Goos and Goarch are the operating system and architectures
	// as reported by the Go runtime.
	Goos, Goarch string
	// Digest is the fingerprint of the currently running binary on the machine.
	Digest digest.Digest
	// TODO: resources
}

// LocalInfo returns system information for this process.
func LocalInfo() Info {
	digestOnce.Do(func() {
		r, err := binary()
		if err != nil {
			log.Error.Printf("could not read local binary: %v", err)
			return
		}
		defer r.Close()
		dw := digester.NewWriter()
		if _, err := io.Copy(dw, r); err != nil {
			log.Error.Print(err)
			return
		}
		binaryDigest = dw.Digest()
	})
	return Info{
		Goos:   runtime.GOOS,
		Goarch: runtime.GOARCH,
		Digest: binaryDigest,
	}
}

type service struct {
	Name     string
	Instance interface{}
}

// Register registers a new service with the machine (server) associated with
// this supervisor. After registration, the service is also initialized if it implements
// the method
//	Init(*B) error
func (s *Supervisor) Register(ctx context.Context, svc service, _ *struct{}) error {
	if err := s.server.Register(svc.Name, svc.Instance); err != nil {
		return err
	}
	return maybeInit(svc.Instance, s.b)
}

// Setargs sets the process' arguments. It should be used before Exec
// in order to invoke the new image with the appropriate arguments.
func (s *Supervisor) Setargs(ctx context.Context, args []string, _ *struct{}) error {
	os.Args = args
	return nil
}

// Setenv sets the processes' environment. It is applied to newly exec'd
// images, and should be called before Exec. The provided environment
// is appended to the default process environment: keys provided here
// override those that already exist in the environment.
func (s *Supervisor) Setenv(ctx context.Context, env []string, _ *struct{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.environ = env
	return nil
}

// Setbinary uploads a new binary to replace the current binary when
// Supervisor.Exec is called. The two calls are separated so that
// different timeouts can be applied to upload and exec.
func (s *Supervisor) Setbinary(ctx context.Context, binary io.Reader, _ *struct{}) error {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, binary); err != nil {
		return err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(path)
		return err
	}
	if err := os.Chmod(path, 0755); err != nil {
		os.Remove(path)
		return err
	}
	s.mu.Lock()
	s.binaryPath = path
	s.mu.Unlock()
	return nil
}

// GetBinary retrieves the last binary uploaded via Setbinary.
func (s *Supervisor) GetBinary(ctx context.Context, _ struct{}, rc *io.ReadCloser) error {
	s.mu.Lock()
	path := s.binaryPath
	s.mu.Unlock()
	if path == "" {
		return errors.E(errors.Invalid, "Supervisor.GetBinary: no binary set")
	}
	f, err := os.Open(path)
	*rc = f
	return err
}

// Exec reads a new image from its argument and replaces the current
// process with it. As a consequence, the currently running machine will
// die. It is up to the caller to manage this interaction.
func (s *Supervisor) Exec(ctx context.Context, _ struct{}, _ *struct{}) error {
	s.mu.Lock()
	var (
		environ = append(os.Environ(), s.environ...)
		path    = s.binaryPath
	)
	s.mu.Unlock()
	if path == "" {
		return errors.E(errors.Invalid, "Supervisor.Exec: no binary set")
	}
	log.Printf("exec %s %s", path, strings.Join(os.Args, " "))
	return syscall.Exec(path, os.Args, environ)
}

// Ping replies immediately with the sequence number provided.
func (s *Supervisor) Ping(ctx context.Context, seq int, replyseq *int) error {
	*replyseq = seq
	return nil
}

// Info returns the info struct for this machine.
func (s *Supervisor) Info(ctx context.Context, _ struct{}, info *Info) error {
	*info = LocalInfo()
	return nil
}

// MemInfo returns system and Go runtime memory usage information.
// Go runtime stats are read if readMemStats is true.
func (s *Supervisor) MemInfo(ctx context.Context, readMemStats bool, info *MemInfo) error {
	if readMemStats {
		runtime.ReadMemStats(&info.Runtime)
	}
	vm, err := mem.VirtualMemory()
	if err != nil {
		return err
	}
	info.System = *vm
	return nil
}

// DiskInfo returns disk usage information on the disk where the
// temporary directory resides.
func (s *Supervisor) DiskInfo(ctx context.Context, _ struct{}, info *DiskInfo) error {
	disk, err := disk.Usage(os.TempDir())
	if err != nil {
		return err
	}
	info.Usage = *disk
	return nil
}

// LoadInfo returns system load information.
func (s *Supervisor) LoadInfo(ctx context.Context, _ struct{}, info *LoadInfo) error {
	load, err := load.AvgWithContext(ctx)
	if err != nil {
		return err
	}
	info.Averages = *load
	return nil
}

// CPUProfile takes a pprof CPU profile of this process for the
// provided duration. If a duration is not provided (is 0) a
// 30-second profile is taken. The profile is returned in the pprof
// serialized form (which uses protocol buffers underneath the hood).
func (s *Supervisor) CPUProfile(ctx context.Context, dur time.Duration, prof *io.ReadCloser) error {
	if dur == time.Duration(0) {
		dur = 30 * time.Second
	}
	if !isContextAliveFor(ctx, dur) {
		return fmt.Errorf("context is too short for duration %s", dur)
	}
	r, w := io.Pipe()
	*prof = r
	go func() {
		if err := pprof.StartCPUProfile(w); err != nil {
			w.CloseWithError(err)
			return
		}
		var err error
		select {
		case <-time.After(dur):
		case <-ctx.Done():
			err = ctx.Err()
		}
		pprof.StopCPUProfile()
		w.CloseWithError(err)
	}()
	return nil
}

type profileRequest struct {
	Name  string
	Debug int
	GC    bool
}

// Profile returns the named pprof profile for the current process.
// The profile is returned in protocol buffer format.
func (s *Supervisor) Profile(ctx context.Context, req profileRequest, prof *io.ReadCloser) error {
	if req.Name == "heap" && req.GC {
		runtime.GC()
	}
	p := pprof.Lookup(req.Name)
	if p == nil {
		return fmt.Errorf("no such profile %s", req.Name)
	}
	r, w := io.Pipe()
	*prof = r
	go func() {
		w.CloseWithError(p.WriteTo(w, req.Debug))
	}()
	return nil
}

type profileStat struct {
	Name  string
	Count int
}

// Profiles returns the set of available profiles and their counts.
func (s *Supervisor) Profiles(ctx context.Context, _ struct{}, profiles *[]profileStat) error {
	for _, p := range pprof.Profiles() {
		*profiles = append(*profiles, profileStat{p.Name(), p.Count()})
	}
	return nil
}

// A keepaliveReply stores the reply to a supervisor keepalive request.
type keepaliveReply struct {
	// Next is the time until the next expected keepalive.
	Next time.Duration
	// Healthy indicates whether the supervisor believes the process to
	// be healthy. An unhealthy process may soon die.
	Healthy bool
}

// Keepalive maintains the machine keepalive. The next argument
// indicates the callers desired keepalive interval (i.e., the amount
// of time until the keepalive expires from the time of the call);
// the accepted time is returned. In order to maintain the keepalive,
// the driver should call Keepalive again before replynext expires.
func (s *Supervisor) Keepalive(ctx context.Context, next time.Duration, reply *keepaliveReply) error {
	now := time.Now()
	defer func() {
		if diff := time.Since(now); diff > 200*time.Millisecond {
			log.Error.Printf("supervisor took a long time to reply to keepalive (%s)", diff)
		}
	}()
	t := now.Add(next)
	select {
	case s.nextc <- t:
		reply.Next = time.Until(t)
		reply.Healthy = atomic.LoadUint32(&s.healthy) != 0
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Getpid returns the PID of the supervisor process.
func (s *Supervisor) Getpid(ctx context.Context, _ struct{}, pid *int) error {
	*pid = os.Getpid()
	return nil
}

// An Expvar is a snapshot of an expvar.
type Expvar struct {
	Key   string
	Value string
}

// Expvars is a collection of snapshotted expvars.
type Expvars []Expvar

type jsonString string

func (s jsonString) MarshalJSON() ([]byte, error) {
	return []byte(s), nil
}

func (e Expvars) MarshalJSON() ([]byte, error) {
	m := make(map[string]jsonString)
	for _, v := range e {
		m[v.Key] = jsonString(v.Value)
	}
	return json.Marshal(m)
}

// Expvars returns a snapshot of this machine's expvars.
func (s *Supervisor) Expvars(ctx context.Context, _ struct{}, vars *Expvars) error {
	expvar.Do(func(kv expvar.KeyValue) {
		*vars = append(*vars, Expvar{kv.Key, kv.Value.String()})
	})
	return nil
}

// TODO(marius): implement a systemd-level watchdog in this routine also.
func (s *Supervisor) watchdog(ctx context.Context) {
	var (
		tick = time.NewTicker(30 * time.Second)
		// Give a generous initial timeout.
		next           = time.Now().Add(2 * time.Minute)
		lastMemProfile time.Time
	)
	for {
		select {
		case <-tick.C:
		case next = <-s.nextc:
		case <-ctx.Done():
			return
		}
		if time.Since(next) > time.Duration(0) {
			log.Error.Printf("Watchdog expiration: next=%s", next.Format(time.RFC3339))
			s.system.Exit(1)
		}
		if time.Since(lastMemProfile) > memProfilePeriod {
			vm, err := mem.VirtualMemory()
			if err != nil {
				// In the case of error, we don't change health status.
				log.Error.Printf("failed to retrieve VM stats: %v", err)
				continue
			}
			if used := vm.UsedPercent; used <= 95 {
				atomic.StoreUint32(&s.healthy, 1)
			} else {
				log.Error.Printf("using %.1f%% of system memory; marking machine unhealthy", used)
				atomic.StoreUint32(&s.healthy, 0)
			}
			lastMemProfile = time.Now()
		}
	}
}

func isContextAliveFor(ctx context.Context, dur time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return dur <= time.Until(deadline)
}
