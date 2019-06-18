// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rpc

import "io"

// SizeTrackingReader keeps track of the number of bytes read
// through the underlying reader.
type sizeTrackingReader struct {
	io.Reader
	n int
}

// Read implements io.Reader.
func (s *sizeTrackingReader) Read(p []byte) (n int, err error) {
	n, err = s.Reader.Read(p)
	s.n += n
	return
}

// Len returns the total number of bytes read from the
// underlying reader.
func (s *sizeTrackingReader) Len() int { return s.n }
