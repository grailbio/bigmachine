// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/iofmt"
	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/internal/authority"
	bigioutil "github.com/grailbio/bigmachine/internal/ioutil"
	"github.com/grailbio/bigmachine/internal/tee"
	"golang.org/x/net/http2"
)

func init() {
	config.Register("bigmachine/local", func(constr *config.Constructor) {
		constr.Doc = "bigmachine/local is the bigmachine instance used for local process-based clusters"
		constr.New = func() (interface{}, error) {
			return Local, nil
		}
	})

	config.Default("bigmachine/system", "bigmachine/local")

	RegisterSystem("local", Local)
}

const maxConcurrentStreams = 20000

// Local is a System that insantiates machines by
// creating new processes on the local machine.
var Local System = new(localSystem)

// LocalSystem implements a System that instantiates machines
// by creating processes on the local machine.
type localSystem struct {
	Gobable           struct{} // to make the struct gob-encodable
	authorityFilename string
	authority         *authority.T

	mu     sync.Mutex
	muxers map[*Machine]*tee.Writer
}

func (s *localSystem) Init(_ *B) error {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	s.authorityFilename = f.Name()
	_ = f.Close()
	if err = os.Remove(s.authorityFilename); err != nil {
		return err
	}
	s.authority, err = authority.New(s.authorityFilename)
	s.muxers = make(map[*Machine]*tee.Writer)
	return err
}

func (*localSystem) Name() string {
	return "local"
}

func (s *localSystem) Start(ctx context.Context, count int) ([]*Machine, error) {
	machines := make([]*Machine, count)
	for i := range machines {
		port, err := getFreeTCPPort()
		if err != nil {
			return nil, err
		}
		prefix := fmt.Sprintf("localhost:%d: ", port)
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Env = os.Environ()
		cmd.Env = append(cmd.Env, "BIGMACHINE_MODE=machine")
		cmd.Env = append(cmd.Env, "BIGMACHINE_SYSTEM=local")
		muxer := new(tee.Writer)
		cmd.Stdout = iofmt.PrefixWriter(muxer, prefix)
		cmd.Stderr = iofmt.PrefixWriter(muxer, prefix)
		cmd.Env = append(cmd.Env, fmt.Sprintf("BIGMACHINE_ADDR=localhost:%d", port))
		cmd.Env = append(cmd.Env, fmt.Sprintf("BIGMACHINE_AUTHORITY=%s", s.authorityFilename))

		m := new(Machine)
		m.Addr = fmt.Sprintf("https://localhost:%d/", port)
		s.mu.Lock()
		s.muxers[m] = muxer
		s.mu.Unlock()
		m.Maxprocs = 1
		err = cmd.Start()
		if err != nil {
			return nil, err
		}
		go func() {
			if err := cmd.Wait(); err != nil {
				log.Printf("machine %s terminated with error: %v", m.Addr, err)
			} else {
				log.Printf("machine %s terminated", m.Addr)
			}
		}()
		machines[i] = m
	}
	return machines, nil
}

func (*localSystem) Main() error {
	var c chan struct{}
	<-c // hang forever
	panic("not reached")
}

func (s *localSystem) Event(typ string, fieldPairs ...interface{}) {
	fields := []string{fmt.Sprintf("eventType:%s", typ)}
	for i := 0; i < len(fieldPairs); i++ {
		name := fieldPairs[i].(string)
		i++
		value := fieldPairs[i]
		fields = append(fields, fmt.Sprintf("%s:%v", name, value))
	}
	log.Debug.Print(strings.Join(fields, ", "))
}

func (s *localSystem) ListenAndServe(addr string, handler http.Handler) error {
	if addr == "" {
		addr = os.Getenv("BIGMACHINE_ADDR")
	}
	if addr == "" {
		return errors.New("no address defined")
	}
	if filename := os.Getenv("BIGMACHINE_AUTHORITY"); filename != "" {
		s.authorityFilename = filename
		var err error
		s.authority, err = authority.New(s.authorityFilename)
		if err != nil {
			return err
		}
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
	err = http2.ConfigureServer(server, &http2.Server{
		MaxConcurrentStreams: maxConcurrentStreams,
	})
	if err != nil {
		return fmt.Errorf("error configuring server: %v", err)
	}
	return server.ListenAndServeTLS("", "")
}

func (s *localSystem) HTTPClient() *http.Client {
	config, _, err := s.authority.HTTPSConfig()
	if err != nil {
		// TODO: propagate error, or return error client
		log.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: config}
	if err = http2.ConfigureTransport(transport); err != nil {
		// TODO: propagate error, or return error client
		log.Fatal(err)
	}
	return &http.Client{Transport: transport}
}

func (*localSystem) Exit(code int) {
	os.Exit(code)
}

func (*localSystem) Shutdown() {}

func (*localSystem) Maxprocs() int {
	return 1
}

func (*localSystem) KeepaliveConfig() (period, timeout, rpcTimeout time.Duration) {
	period = time.Minute
	timeout = 2 * time.Minute
	rpcTimeout = 10 * time.Second
	return
}

func (s *localSystem) Tail(ctx context.Context, m *Machine) (io.Reader, error) {
	s.mu.Lock()
	muxer := s.muxers[m]
	s.mu.Unlock()
	if muxer == nil {
		return nil, errors.New("machine not under management")
	}
	r, w := io.Pipe()
	go func() {
		cancel := muxer.Tee(w)
		<-ctx.Done()
		cancel()
		w.CloseWithError(ctx.Err())
	}()
	return r, nil
}

func (s *localSystem) Read(ctx context.Context, m *Machine, filename string) (io.Reader, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return bigioutil.NewClosingReader(f), nil
}

func getFreeTCPPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}
