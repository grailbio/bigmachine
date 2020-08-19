// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/grailbio/base/log"
	"golang.org/x/sync/errgroup"
)

type machineVars struct{ *B }

// String returns a JSON-formatted string representing the exported
// variables of all underlying machines.
//
// TODO(marius): aggregate values too?
func (v machineVars) String() string {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	var (
		mu   sync.Mutex
		vars = make(map[string]Expvars)
	)
	for _, m := range v.B.Machines() {
		// Only propagate stats for machines we own, otherwise we can
		// create stats loops.
		if !m.Owned() {
			continue
		}

		m := m
		g.Go(func() error {
			var mvars Expvars
			if err := m.Call(ctx, "Supervisor.Expvars", struct{}{}, &mvars); err != nil {
				log.Error.Printf("failed to retrieve variables for %s: %v", m.Addr, err)
				return nil
			}
			mu.Lock()
			vars[m.Addr] = mvars
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		b, errMarshal := json.Marshal(err.Error())
		if errMarshal != nil {
			log.Error.Printf("machineVars marshal: %v", errMarshal)
			return `"error"`
		}
		return string(b)
	}
	b, err := json.Marshal(vars)
	if err != nil {
		log.Error.Printf("machineVars marshal: %v", err)
		return `"error"`
	}
	return string(b)
}
