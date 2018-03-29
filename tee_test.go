// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
)

func tostring(r io.Reader) string {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestTee(t *testing.T) {
	tee := newTee(1024)
	var b1, b2 bytes.Buffer
	tee.Tee(&b1)
	tee.Tee(&b2)
	expect := "hello world"
	io.WriteString(tee, expect)
	tee.Flush()
	if got, want := tostring(&b1), expect; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tostring(&b2), expect; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestTeeDrop(t *testing.T) {
	tee := newTee(1024)
	r, w := io.Pipe()
	tee.Tee(w)
	rnd := rand.New(rand.NewSource(rand.Int63()))
	in := make([]byte, 1000)
	if _, err := rnd.Read(in); err != nil {
		t.Fatal(err)
	}
	tee.Write(in)
	tee.Write(in)
	var out []byte
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		var err error
		out, err = ioutil.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		wg.Done()
	}()
	tee.Flush()
	w.Close()
	wg.Wait()

	if !bytes.Equal(in, out) {
		t.Errorf("buffers differ (in=%d, out=%d)", len(in), len(out))
	}
}
