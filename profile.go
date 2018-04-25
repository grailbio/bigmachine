// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/pprof/profile"
	"github.com/grailbio/base/log"
	"golang.org/x/sync/errgroup"
)

// ProfileHandler implements an HTTP handler for a profile. The
// handler gathers profiles from all machines (at the time of
// collection) and returns a merged profile representing all cluster
// activity.
type profileHandler struct {
	b     *B
	which string
}

func (p *profileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sec, _ := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	if sec == 0 {
		sec = 30
	}
	g, ctx := errgroup.WithContext(r.Context())
	var mu sync.Mutex
	var profiles []*profile.Profile
	for _, m := range p.b.Machines() {
		if m.State() != Running {
			continue
		}
		m := m
		g.Go(func() error {
			var rc io.ReadCloser
			if p.which == "profile" {
				if err := m.Call(ctx, "Supervisor.CPUProfile", time.Duration(sec)*time.Second, &rc); err != nil {
					log.Error.Printf("failed to collect profile from %s: %v", m.Addr, err)
					return nil
				}
			} else {
				if err := m.Call(ctx, "Supervisor.Profile", p.which, &rc); err != nil {
					log.Error.Printf("failed to collect profile from %s: %v", m.Addr, err)
				}
			}
			defer rc.Close()
			prof, err := profile.Parse(rc)
			if err != nil {
				log.Error.Printf("failed to parse profile from %s: %v", m.Addr, err)
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
