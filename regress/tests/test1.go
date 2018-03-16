// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"context"
	"log"

	"github.com/grailbio/bigmachine"
)

type service struct{}

func (service) Strlen(ctx context.Context, arg string, reply *int) error {
	*reply = len(arg)
	return nil
}

func main() {
	svc := bigmachine.Register(service{})
	bigmachine.Run()
	ctx := context.Background()
	m, err := bigmachine.Start(ctx)
	if err != nil {
		log.Fatal(err)
	}
	<-m.Wait(bigmachine.Running)
	const str = "hello world"
	var n int
	if err := m.Call(ctx, svc, "Strlen", str, &n); err != nil {
		log.Fatal(err)
	}
	if got, want := n, len(str); got != want {
		log.Fatalf("got %v, want %v", got, want)
	}
}
