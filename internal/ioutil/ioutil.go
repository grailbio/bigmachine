// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package ioutil contains utilities for performing I/O in bigmachine.
package ioutil

import "io"

type closingReader struct {
	rc     io.ReadCloser
	closed bool
}

// NewClosingReader returns a reader that closes the provided
// ReadCloser once it is read through EOF.
func NewClosingReader(rc io.ReadCloser) io.Reader {
	return &closingReader{rc, false}
}

func (c *closingReader) Read(p []byte) (n int, err error) {
	if c.closed {
		return 0, io.EOF
	}
	n, err = c.rc.Read(p)
	if err == io.EOF {
		c.rc.Close()
		c.closed = true
	}
	return
}
