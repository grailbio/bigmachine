// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"io"
	"sync"
)

type teeWriter struct {
	w         io.Writer
	maxBuffer int
	mu        sync.Mutex
	cond      *sync.Cond
	bufs      [][]byte
	pending   int
	err       error
}

func newTeeWriter(w io.Writer, maxBuffer int) *teeWriter {
	tw := &teeWriter{w: w, maxBuffer: maxBuffer}
	tw.cond = sync.NewCond(&tw.mu)
	return tw
}

func (w *teeWriter) Go() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for w.err == nil {
		for len(w.bufs) == 0 {
			w.cond.Wait()
		}
		buf := w.bufs[0]
		w.bufs = w.bufs[1:]
		w.mu.Unlock()
		_, err := w.w.Write(buf)
		w.mu.Lock()
		w.err = err
		w.pending -= len(buf)
		w.cond.Broadcast()
	}
	return w.err
}

func (w *teeWriter) Enqueue(p []byte) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return false
	}
	if len(p)+w.pending > w.maxBuffer {
		return false
	}
	w.pending += len(p)
	w.bufs = append(w.bufs, p)
	w.cond.Broadcast()
	return true
}

func (w *teeWriter) Flush() {
	w.mu.Lock()
	for w.err == nil && w.pending > 0 {
		w.cond.Wait()
	}
	w.mu.Unlock()
}

// Tee is an io.Writer that copies its output to zero or more
// underlying writers. The underlying writes performed
// asynchronously; data are buffered up to a configured limit, after
// which subsequent writes are dropped until the buffer shrinks below
// the threshold again. Tee is intended for diagnostic output such as
// logs.
type tee struct {
	mu        sync.Mutex
	writers   map[*teeWriter]bool
	maxBuffer int
}

// NewTee creates a new tee with the provided maximum buffer limit.
func newTee(maxBuffer int) *tee {
	return &tee{
		writers:   make(map[*teeWriter]bool),
		maxBuffer: maxBuffer,
	}
}

// Tee starts teeing writes to the provided w. Writes continue until
// the writer w fails.
func (t *tee) Tee(w io.Writer) {
	tw := newTeeWriter(w, t.maxBuffer)
	t.mu.Lock()
	t.writers[tw] = true
	t.mu.Unlock()
	go func() {
		_ = tw.Go()
		// TODO(marius): Log the returner error. The tricky part is that
		// logging may trigger a write. Perhaps there should be a
		// "distinguished writer" to which we can log safely.
		t.mu.Lock()
		delete(t.writers, tw)
		t.mu.Unlock()
	}()
}

// Flush returns after all buffered content has been written to all
// currently teeing writers.
func (t *tee) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for tw := range t.writers {
		tw.Flush()
	}
}

// Write asynchronously enqueues writes to all the current writers,
// applying buffer limits as described above. Write always returns
// len(p), nil.
func (t *tee) Write(p []byte) (n int, err error) {
	buf := make([]byte, len(p))
	copy(buf, p)
	t.mu.Lock()
	for tw := range t.writers {
		tw.Enqueue(buf)
		// TODO(marius): log this, as above.
	}
	t.mu.Unlock()
	return len(p), nil
}
