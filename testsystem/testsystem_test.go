// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package testsystem

import (
	"context"
	"encoding/gob"
	"testing"

	"github.com/grailbio/bigmachine"
)

func init() {
	gob.Register(&testService{})
}

type testService struct {
	Index int
}

func (t *testService) Method(ctx context.Context, arg int, reply *int) error {
	*reply = t.Index
	return nil
}

func TestTestSystem(t *testing.T) {
	test := New()
	b := bigmachine.Start(test)
	defer b.Shutdown()
	ctx := context.Background()
	machines, err := b.Start(ctx, 1, bigmachine.Services{
		"Service": &testService{Index: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	m := machines[0]
	<-m.Wait(bigmachine.Running)
	var reply int
	if err := m.Call(ctx, "Service.Method", 0, &reply); err != nil {
		t.Fatal(err)
	}
	if got, want := reply, 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
