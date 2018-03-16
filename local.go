// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
)

// LocalSystem implements a System that instantiates machines
// by creating processes on the local machine.
type localSystem struct{}

func (localSystem) Init() error {
	return nil
}

func (localSystem) Name() string {
	return "local"
}

func (localSystem) Start(ctx context.Context) (*Machine, error) {
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "BIGMACHINE_MODE=machine")
	port, err := getFreeTCPPort()
	if err != nil {
		return nil, err
	}
	cmd.Env = append(cmd.Env, fmt.Sprintf("BIGMACHINE_ADDR=:%d", port))
	m := new(Machine)
	m.Addr = fmt.Sprintf("http://localhost:%d/", port)
	m.Maxprocs = 1
	return m, cmd.Start()
}

func (localSystem) Main() error {
	addr := os.Getenv("BIGMACHINE_ADDR")
	if addr == "" {
		return errors.New("no address defined")
	}
	return http.ListenAndServe(addr, nil)
}

func (localSystem) HTTPClient() *http.Client {
	return nil
}

func (localSystem) Exit(code int) {
	os.Exit(code)
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
