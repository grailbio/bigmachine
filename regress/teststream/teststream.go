// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"io"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"time"

	"github.com/grailbio/base/log"
	"github.com/grailbio/bigmachine"
	"golang.org/x/net/http2"
)

func init() {
	gob.Register(service{})
}

type service struct{}

func (service) Empty(ctx context.Context, howlong time.Duration, reply *io.ReadCloser) error {
	http2.VerboseLogs = true
	go func() {
		if err := http.ListenAndServe("localhost:8090", nil); err != nil {
			log.Fatal(err)
		}
	}()
	*reply = ioutil.NopCloser(bytes.NewReader(nil))
	return nil
}

func main() {
	b := bigmachine.Start(bigmachine.Local)
	defer b.Shutdown()
	ctx := context.Background()
	machines, err := b.Start(ctx, 1, bigmachine.Services{
		"Service": service{},
	})
	if err != nil {
		log.Fatal(err)
	}
	m := machines[0]
	<-m.Wait(bigmachine.Running)
	var rc io.ReadCloser
	if err = m.Call(ctx, "Service.Empty", time.Second, &rc); err != nil {
		log.Fatal(err)
	}
	go func() {
		time.Sleep(3 * time.Second)
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		log.Fatal("should be dead by now")
	}()
	if _, err := io.Copy(ioutil.Discard, rc); err != nil {
		log.Fatal(err)
	}
}
