// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rpc

import (
	"expvar"
	"sync"
	"time"
)

var serverstats, clientstats rpcstats

func init() {
	expvar.Publish("server", &serverstats)
	expvar.Publish("client", &clientstats)
}

// A treestats represents a tree of expvars.
type treestats struct {
	expvar.Map
	mu sync.Mutex
}

// Path returns the treestats with the provided path.
func (t *treestats) Path(names ...string) *treestats {
	child := t
	for _, name := range names {
		child = child.Child(name)
	}
	return child
}

// Child returns the treestat's child with the given path,
// creating one if it does not yet exist.
func (t *treestats) Child(name string) *treestats {
	child, ok := t.Map.Get(name).(*treestats)
	if child != nil && ok {
		return child
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	child, ok = t.Map.Get(name).(*treestats)
	if child != nil && ok {
		return child
	}
	child = new(treestats)
	t.Map.Set(name, child)
	return child
}

// Rpcstats maintains simple RPC statistics, aggregated by address
// and method.
type rpcstats struct {
	treestats
}

// Start starts an RPC stat with the provided address and method. The
// stat is timed and is recorded when the returned completion
// function is called.
func (r *rpcstats) Start(addr, method string) (done func()) {
	r.Path("method", method).Add("count", 1)
	if addr != "" {
		r.Path("machine", addr, "method", method).Add("count", 1)
	}
	now := time.Now()
	return func() {
		elapsed := int64(time.Since(now).Nanoseconds()) / 1e6
		r.Path("method", method).Add("time", elapsed)
		r.max(elapsed, "method", method, "maxtime")

		if addr != "" {
			r.Path("machine", addr, "method", method).Add("time", elapsed)
			r.max(elapsed, "machine", addr, "method", method, "maxtime")
		}
	}
}

func (r *rpcstats) max(val int64, path ...string) {
	path, name := path[:len(path)-1], path[len(path)-1]
	r.Path(path...).Add(name, 0)
	if iv, ok := r.Path(path...).Get(name).(*expvar.Int); ok {
		if val > iv.Value() {
			iv.Set(val)
		}
	}
}
