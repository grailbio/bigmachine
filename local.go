// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine/bigioutil"
	"github.com/grailbio/bigmachine/internal/authority"
	"golang.org/x/net/http2"
)

const maxConcurrentStreams = 20000
const httpTimeout = 30 * time.Second

// Local is a System that insantiates machines by
// creating new processes on the local machine.
var Local System = new(localSystem)

// LocalSystem implements a System that instantiates machines
// by creating processes on the local machine.
type localSystem struct {
	authorityFilename string
	authority         *authority.T
}

func (s *localSystem) Init(_ *B) error {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	s.authorityFilename = f.Name()
	_ = f.Close()
	if err := os.Remove(s.authorityFilename); err != nil {
		return err
	}
	s.authority, err = authority.New(s.authorityFilename)
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
		cmd.Stdout = bigioutil.PrefixWriter(os.Stdout, prefix)
		cmd.Stderr = bigioutil.PrefixWriter(os.Stderr, prefix)
		cmd.Env = append(cmd.Env, fmt.Sprintf("BIGMACHINE_ADDR=:%d", port))
		cmd.Env = append(cmd.Env, fmt.Sprintf("BIGMACHINE_AUTHORITY=%s", s.authorityFilename))

		m := new(Machine)
		m.Addr = fmt.Sprintf("https://localhost:%d/", port)
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
	http2.ConfigureServer(server, &http2.Server{
		MaxConcurrentStreams: maxConcurrentStreams,
	})
	return server.ListenAndServeTLS("", "")
}

func (s *localSystem) HTTPClient() *http.Client {
	config, _, err := s.authority.HTTPSConfig()
	if err != nil {
		// TODO: propagate error, or return error client
		log.Fatal(err)
	}
	transport := &http.Transport{TLSClientConfig: config}
	http2.ConfigureTransport(transport)
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

func getFreeTCPPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
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
