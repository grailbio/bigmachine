// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/grailbio/bigmachine/rpc"
)

var fakeDigest = digester.FromString("fake binary")

type fakeSupervisor struct {
	Args          []string
	Environ       []string
	Image         []byte
	LastKeepalive time.Time
	Hung          bool
}

func (s *fakeSupervisor) Setenv(ctx context.Context, env []string, _ *struct{}) error {
	s.Environ = env
	return nil
}

func (s *fakeSupervisor) Setargs(ctx context.Context, args []string, _ *struct{}) error {
	s.Args = args
	return nil
}

func (s *fakeSupervisor) Exec(ctx context.Context, exec io.Reader, _ *struct{}) error {
	var err error
	s.Image, err = ioutil.ReadAll(exec)
	return err
}

func (s *fakeSupervisor) Tail(ctx context.Context, fd int, rc *io.ReadCloser) error {
	return errors.New("not supported")
}

func (s *fakeSupervisor) Ping(ctx context.Context, seq int, replyseq *int) error {
	*replyseq = seq
	return nil
}

func (s *fakeSupervisor) Info(ctx context.Context, _ struct{}, info *Info) error {
	info.Goos = runtime.GOOS
	info.Goarch = runtime.GOARCH
	info.Digest = fakeDigest
	return nil
}

func (s *fakeSupervisor) Keepalive(ctx context.Context, next time.Duration, reply *keepaliveReply) error {
	if s.Hung {
		<-ctx.Done()
		return ctx.Err()
	}
	s.LastKeepalive = time.Now()
	reply.Next = next
	reply.Healthy = true
	return nil
}

func (s *fakeSupervisor) Hang(ctx context.Context, _ struct{}, _ *struct{}) error {
	<-ctx.Done()
	return ctx.Err()
}

func newTestMachine(t *testing.T, params ...Param) (m *Machine, supervisor *fakeSupervisor, shutdown func()) {
	t.Helper()
	supervisor = new(fakeSupervisor)
	srv := rpc.NewServer()
	srv.Register("Supervisor", supervisor)
	httpsrv := httptest.NewServer(srv)
	client, err := rpc.NewClient(func() *http.Client { return httpsrv.Client() }, "/")
	if err != nil {
		httpsrv.Close()
		t.Fatal(err)
	}
	m = &Machine{
		Addr:                httpsrv.URL,
		client:              client,
		owner:               true,
		keepalivePeriod:     time.Minute,
		keepaliveTimeout:    2 * time.Minute,
		keepaliveRpcTimeout: 10 * time.Second,
	}
	for _, param := range params {
		param.applyParam(m)
	}
	m.start(nil)
	return m, supervisor, func() {
		m.Cancel()
		select {
		case <-m.Wait(Stopped):
		case <-time.After(time.Second):
			t.Log("failed to stop server after 1 second")
		}
		httpsrv.Close()
	}
}

func TestMachineBootup(t *testing.T) {
	m, supervisor, shutdown := newTestMachine(t)
	defer shutdown()

	<-m.Wait(Running)
	if got, want := m.State(), Running; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	r, err := binary()
	if err != nil {
		t.Fatal(err)
	}
	image, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(supervisor.Image, image) {
		t.Error("image does not match")
	}
	if time.Since(supervisor.LastKeepalive) > time.Minute {
		t.Errorf("failed to maintain keepalive")
	}
}

func TestMachineEnv(t *testing.T) {
	m, supervisor, shutdown := newTestMachine(t, Environ{"test=yes"})
	defer shutdown()
	<-m.Wait(Running)
	if got, want := len(supervisor.Environ), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if got, want := supervisor.Environ[0], "test=yes"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestCallTimeout(t *testing.T) {
	m, _, shutdown := newTestMachine(t)
	defer shutdown()
	const timeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err := m.Call(ctx, "Supervisor.Hang", struct{}{}, nil)
	if got, want := err, context.DeadlineExceeded; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	cancel()
}

func TestMachineContext(t *testing.T) {
	log.SetFlags(log.Llongfile)
	m, supervisor, shutdown := newTestMachine(t)
	defer shutdown()
	supervisor.Hung = true
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()
	err := m.Call(ctx, "Supervisor.Hang", struct{}{}, nil)
	if got, want := err, context.Canceled; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
