// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"crypto"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/bigmachine/rpc"
)

const maxTeeBuffer = 1 << 20

var (
	digester     = digest.Digester(crypto.SHA256)
	binaryDigest digest.Digest
	digestOnce   sync.Once
)

// LocalSupervisor is the local instance of the supervisor service.
var LocalSupervisor *Supervisor = new(Supervisor)

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
	nextc                chan time.Time
	saveFds              map[int]int
	stdoutTee, stderrTee *tee
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

// Init starts the supervisor and the supervisor's watchdog. The
// watchdog kills the process if it is not kept alive at regular
// intervals.
func (s *Supervisor) Init(_ *Service) error {
	if Driver() {
		return nil
	}
	s.saveFds = make(map[int]int)
	// Capture output stdout and stderr.
	var err error
	s.stderrTee, err = s.teeFd(syscall.Stderr, "/dev/stderr")
	if err != nil {
		log.Printf("failed to tee stderr: %v", err)
	}
	s.stdoutTee, err = s.teeFd(syscall.Stdout, "/dev/stdout")
	if err != nil {
		log.Printf("failed to tee stdout: %v", err)
	}
	s.nextc = make(chan time.Time)
	go s.watchdog()
	return nil
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
			log.Printf("dup2 %d %d: %v", save, orig, err)
		}
		if err := syscall.Close(save); err != nil {
			log.Printf("close %d: %v", save, err)
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
	digestOnce.Do(func() {
		r, err := binary()
		if err != nil {
			log.Printf("could not read local binary: %v", err)
			return
		}
		defer r.Close()
		dw := digester.NewWriter()
		if _, err := io.Copy(dw, r); err != nil {
			log.Print(err)
			return
		}
		binaryDigest = dw.Digest()
	})
	info.Goos = runtime.GOOS
	info.Goarch = runtime.GOARCH
	info.Digest = binaryDigest
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

// Profile returns the named pprof profile for the current process.
// The profile is returned in protocol buffer format.
//
// TODO(marius): once bigmachine/rpc supports protobuf encoding,
// just return the protobuf directly here.
func (s *Supervisor) Profile(ctx context.Context, name string, prof *io.ReadCloser) error {
	p := pprof.Lookup(name)
	if p == nil {
		return fmt.Errorf("no such profile %s", name)
	}
	r, w := io.Pipe()
	*prof = r
	go func() {
		w.CloseWithError(p.WriteTo(w, 0))
	}()
	return nil
}

// Keepalive maintains the machine keepalive. The next argument
// indicates the callers desired keepalive interval (i.e., the amount
// of time until the keepalive expires from the time of the call);
// the accepted time is returned. In order to maintain the keepalive,
// the driver should call Keepalive again before replynext expires.
func (s *Supervisor) Keepalive(ctx context.Context, next time.Duration, replynext *time.Duration) error {
	t := time.Now().Add(next)
	select {
	case s.nextc <- t:
		*replynext = time.Until(t)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Supervisor) watchdog() {
	tick := time.NewTicker(30 * time.Second)
	// Give a generous initial timeout.
	next := time.Now().Add(2 * time.Minute)
	for {
		select {
		case <-tick.C:
		case next = <-s.nextc:
		}
		if time.Since(next) > time.Duration(0) {
			Impl.Exit(1)
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
			log.Printf("tee %d %s: %v", fd, name, err)
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
