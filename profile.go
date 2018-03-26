// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"golang.org/x/sync/errgroup"
)

// ProfileHandler implements an HTTP handler for a profile. The
// handler gathers profiles from all machines (at the time of
// collection) and returns a merged profile representing all cluster
// activity.
type profileHandler string

func (name profileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sec, _ := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec == 0 {
		sec = 30
	}
	stateMu.Lock()
	snapshot := make([]*Machine, 0, len(machines))
	for _, machine := range machines {
		snapshot = append(snapshot, machine)
	}
	stateMu.Unlock()
	g, ctx := errgroup.WithContext(r.Context())
	var mu sync.Mutex
	var profiles []*profile.Profile
	for _, m := range snapshot {
		m := m
		g.Go(func() error {
			var rc io.ReadCloser
			if name == "profile" {
				if err := m.Call(ctx, supervisor, "CPUProfile", time.Duration(sec)*time.Second, &rc); err != nil {
					log.Printf("failed to collect profile from %s: %v", m.Addr, err)
					return nil
				}
			} else {
				if err := m.Call(ctx, supervisor, "Profile", name, &rc); err != nil {
					log.Printf("failed to collect profile from %s: %v", m.Addr, err)
				}
			}
			defer rc.Close()
			prof, err := profile.Parse(rc)
			if err != nil {
				log.Printf("failed to parse profile from %s: %v", m.Addr, err)
				return nil
			}
			mu.Lock()
			profiles = append(profiles, prof)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		profileErrorf(w, http.StatusInternalServerError, "failed to fetch profiles: %v", err)
		return
	}
	if len(profiles) == 0 {
		profileErrorf(w, http.StatusNotFound, "no profiles are available at this time")
		return
	}
	prof, err := profile.Merge(profiles)
	if err != nil {
		profileErrorf(w, http.StatusInternalServerError, "profile merge error: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := prof.Write(w); err != nil {
		profileErrorf(w, http.StatusInternalServerError, "failed to write profile: %v", err)
	}
}

func profileErrorf(w http.ResponseWriter, code int, message string, args ...interface{}) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.WriteHeader(code)
	fmt.Fprintf(w, message, args...)
}
