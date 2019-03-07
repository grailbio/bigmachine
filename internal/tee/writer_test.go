// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tee

import (
	"io"
	"io/ioutil"
	"sync"
	"testing"
)

func write(t *testing.T, w io.Writer, p string) {
	t.Helper()
	if _, err := io.WriteString(w, p); err != nil {
		t.Fatal(err)
	}
}

func read(t *testing.T, r io.Reader, want string) {
	t.Helper()
	p := make([]byte, len(want))
	if _, err := io.ReadFull(r, p); err != nil {
		t.Fatal(err)
	}
	if got := string(p); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestWriter(t *testing.T) {
	r, w := io.Pipe()
	tee := new(Writer)
	cancel := tee.Tee(w)
	_ = tee.Tee(ioutil.Discard)

	// This should not block, and should not discard.
	write(t, tee, "hello, world")
	write(t, tee, "hi there")

	read(t, r, "hello, worldhi there")

	cancel()
	write(t, tee, "into the void")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		read(t, r, ".")
		wg.Done()
	}()
	write(t, w, ".")
	wg.Wait()
}

func TestWriterDiscard(t *testing.T) {
	r, w := io.Pipe()
	tee := new(Writer)
	cancel := tee.Tee(w)
	defer cancel()
	write(t, tee, "hello")
	buf := make([]byte, bufferSize)
	read(t, r, "hel") // make sure that we start reading the buffered write
	write(t, tee, string(buf))
	write(t, tee, "hello world")
	read(t, r, "lo"+string(buf[:len(buf)-len("hello world")])+"hello world")
}
