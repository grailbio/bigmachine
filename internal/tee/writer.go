// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package tee implements utilities for I/O multiplexing.
package tee

import (
	"bytes"
	"io"
	"sync"
)

const bufferSize = 512 << 10

// Write is an asynchronous write multiplexer. Write maintains a
// small internal buffer but may discard writes for writers that
// cannot keep up; thus it is meant for log output or similar.
type Writer sync.Map

// Tee forwards future writes from this writer to wr; forwarding
// stops after the return cancelation function is invoked. A small
// buffer is maintained for each writer, but writes are dropped if
// the writer wr cannot keep up with the write volume. The writer is
// not forwarded any more writes if returns an error.
func (w *Writer) Tee(wr io.Writer) (cancel func()) {
	c := make(chan *bytes.Buffer, 1)
	(*sync.Map)(w).Store(c, nil)
	done := make(chan struct{})
	var once sync.Once
	cancel = func() {
		once.Do(func() {
			(*sync.Map)(w).Delete(c)
			close(done)
		})
	}
	go func() {
		for {
			select {
			case <-done:
				return
			case buf := <-c:
				if _, err := io.Copy(wr, buf); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	return cancel
}

// Write writes p to each writer that is managed by this multiplexer.
// Write is asynchronous, and always returns len(p), nil.
func (w *Writer) Write(p []byte) (n int, err error) {
	(*sync.Map)(w).Range(func(k, v interface{}) bool {
		c := k.(chan *bytes.Buffer)
		var buf *bytes.Buffer
		select {
		case buf = <-c:
		default:
			buf = new(bytes.Buffer)
		}
		// Discard extra bytes in the case of buffer overflow.
		if n := buf.Len() + len(p) - bufferSize; n > 0 {
			buf.Next(n)
		}
		buf.Write(p)
		c <- buf
		return true
	})
	return len(p), nil
}
