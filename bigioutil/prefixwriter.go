// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package bigioutil contains various IO utilities used within the Bigmachine
// implementation.
package bigioutil

import (
	"bytes"
	"io"
)

var newline = []byte{'\n'}

// prefixWriter is an io.Writer that outputs a prefix before each line.
type prefixWriter struct {
	w          io.Writer
	prefix     string
	needPrefix bool
}

// PrefixWriter returns a new io.Writer that copies its writes
// to the provided io.Writer, adding a prefix at the beginning
// of each line.
func PrefixWriter(w io.Writer, prefix string) io.Writer {
	return &prefixWriter{w: w, prefix: prefix, needPrefix: true}
}

func (w *prefixWriter) Write(p []byte) (n int, err error) {
	if w.needPrefix {
		if _, err := io.WriteString(w.w, w.prefix); err != nil {
			return 0, err
		}
		w.needPrefix = false
	}
	for {
		i := bytes.Index(p, newline)
		switch i {
		case len(p) - 1:
			w.needPrefix = true
			fallthrough
		case -1:
			m, err := w.w.Write(p)
			return n + m, err
		default:
			m, err := w.w.Write(p[:i+1])
			n += m
			if err != nil {
				return n, err
			}
			_, err = io.WriteString(w.w, w.prefix)
			if err != nil {
				return n, err
			}
			p = p[i+1:]
		}
	}
}
