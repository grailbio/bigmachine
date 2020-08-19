// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Command bigoom causes a bigmachine instance to OOM. The sole purpose
// of this binary is to test bigmachine's OOM handling.
package main

import (
	"context"
	"encoding/gob"
	"flag"
	"net/http"

	"github.com/grailbio/base/log"
	"github.com/grailbio/base/stress/oom"
	"github.com/grailbio/bigmachine"
	"github.com/grailbio/bigmachine/driver"
)

func init() {
	gob.Register(oomer{})
}

type oomer struct{}

func (oomer) Try(ctx context.Context, _ struct{}, _ *struct{}) error {
	oom.Try()
	panic("not reached")
}

func main() {
	log.AddFlags()
	flag.Parse()
	b := driver.Start()
	defer b.Shutdown()

	go func() {
		err := http.ListenAndServe(":3333", nil)
		log.Printf("http.ListenAndServe: %v", err)
	}()
	ctx := context.Background()
	services := bigmachine.Services{
		"OOM": oomer{},
	}
	machines, err := b.Start(ctx, 1, services)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("waiting for machines to come online")
	m := machines[0]
	<-m.Wait(bigmachine.Running)
	log.Printf("machine %s %s", m.Addr, m.State())
	if err = m.Err(); err != nil {
		log.Fatal(err)
	}

	err = m.RetryCall(ctx, "OOM.Try", struct{}{}, nil)
	log.Printf("call error: %v", err)
	log.Printf("machine error: %v", m.Err())
}
