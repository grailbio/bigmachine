// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rpc

import (
	"github.com/grailbio/base/log"
	"golang.org/x/time/rate"
)

// RateLimitingOutputter is a log.Outputter that enforces a rate
// limit on outputted messages. Messages that are logged beyond
// the allowed rate are dropped.
type rateLimitingOutputter struct {
	*rate.Limiter
	log.Outputter
}

// Output implements log.Outputter.
func (r *rateLimitingOutputter) Output(calldepth int, level log.Level, s string) error {
	if !r.Limiter.Allow() {
		return nil
	}
	return r.Outputter.Output(calldepth+1, level, s)
}
