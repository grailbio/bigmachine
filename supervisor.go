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
	b                    *B
	system               System
	server               *rpc.Server
	nextc                chan time.Time
	saveFds              map[int]int
	stdoutTee, stderrTee *tee
	healthy              uint32
}

// StartSupervisor starts a new supervisor based on the provided arguments.
// The redirect parameter determines whether the supervisor should capture
// the process's standard IO for calls to Supervisor.Tail.
func StartSupervisor(b *B, system System, server *rpc.Server, redirect bool) *Supervisor {
	s := &Supervisor{
		b:       b,
		system:  system,
		server:  server,
		saveFds: make(map[int]int),
	}
	if redirect {
		var err error
		s.stderrTee, err = s.teeFd(syscall.Stderr, "/dev/stderr")
		if err != nil {
			log.Error.Printf("failed to tee stderr: %v", err)
		}
		s.stdoutTee, err = s.teeFd(syscall.Stdout, "/dev/stdout")
		if err != nil {
			log.Error.Printf("failed to tee stdout: %v", err)
		}
	}
	s.healthy = 1
	s.nextc = make(chan time.Time)
	go s.watchdog()
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

// Exec reads a new image from its argument and replaces the current
// process with it. As a consequence, the currently running machine will
// die. It is up to the caller to manage this interaction.
//
// TODO(marius): replicate the relevant parts of the caller's environment.
func (s *Supervisor) Exec(ctx context.Context, exec io.Reader, _ *struct{}) error {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, exec); err != nil {
		return err
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Chmod(path, 0755); err != nil {
		return err
	}
	log.Printf("exec %s %s", path, strings.Join(os.Args, " "))
	// Restore original fds so that that they can be re-duped again
	// by our replacement.
	for orig, save := range s.saveFds {
		if err := syscall.Dup2(save, orig); err != nil {
			log.Error.Printf("dup2 %d %d: %v", save, orig, err)
		}
		if err := syscall.Close(save); err != nil {
			log.Error.Printf("close %d: %v", save, err)
		}
	}
	return syscall.Exec(path, os.Args, os.Environ())
}

// Tail returns a readcloser to which the output of the argument
// file descriptor is copied. This can be used to write standard
// output and error to the console of another machine.
func (s *Supervisor) Tail(ctx context.Context, fd int, rc *io.ReadCloser) error {
	var tee *tee
	switch fd {
	case syscall.Stdout:
		tee = s.stdoutTee
	case syscall.Stderr:
		tee = s.stderrTee
	}
	if tee == nil {
		return fmt.Errorf("cannot tail fd %d", fd)
	}
	r, w := io.Pipe()
	tee.Tee(w)
	*rc = rpc.Flush(r)
	return nil
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
}

// Profile returns the named pprof profile for the current process.
// The profile is returned in protocol buffer format.
func (s *Supervisor) Profile(ctx context.Context, req profileRequest, prof *io.ReadCloser) error {
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
func (s *Supervisor) watchdog() {
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
		}
		if time.Since(next) > time.Duration(0) {
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
				log.Error.Printf("using %d%% of system memory; marking machine unhealthy", used)
				atomic.StoreUint32(&s.healthy, 0)
			}
			lastMemProfile = time.Now()
		}
	}
}

// TeeFd redirects the fd to a tee that is returned. The original fd
// is added to the tee. Not thread safe: should only be called during
// initialization.
func (s *Supervisor) teeFd(fd int, name string) (*tee, error) {
	savefd, err := syscall.Dup(fd)
	if err != nil {
		return nil, err
	}
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	if err := syscall.Dup2(int(w.Fd()), fd); err != nil {
		return nil, err
	}
	s.saveFds[fd] = savefd
	tee := newTee(maxTeeBuffer)
	tee.Tee(os.NewFile(uintptr(savefd), name))
	go func() {
		_, err := io.Copy(tee, r)
		if err != nil {
			log.Error.Printf("tee %d %s: %v", fd, name, err)
		}
	}()
	return tee, nil
}
func isContextAliveFor(ctx context.Context, dur time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return dur <= time.Until(deadline)
}
