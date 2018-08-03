// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sort"
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
	sec64, _ := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
	sec := int(sec64)
	if sec == 0 {
		sec = 30
	}
	debug, _ := strconv.Atoi(r.FormValue("debug"))
	// If a machine is specified, we pass through the profile directly.
	if addr := r.FormValue("machine"); addr != "" {
		ctx := r.Context()
		m, err := p.b.Dial(ctx, addr)
		if err != nil {
			profileErrorf(w, http.StatusInternalServerError, "failed to dial machine: %v", err)
			return
		}
		rc, err := getProfile(ctx, m, p.which, sec, debug)
		if err != nil {
			profileErrorf(w, http.StatusInternalServerError, "failed to collect %s profile: %v", p.which, err)
			return
		}
		defer rc.Close()
		if debug > 0 && p.which != "profile" {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		} else {
			w.Header().Set("Content-Type", "application/octet-stream")
		}
		_, err = io.Copy(w, rc)
		if err != nil {
			profileErrorf(w, http.StatusInternalServerError, "failed to write %s profile: %v", p.which, err)
		}
		return
	}

	g, ctx := errgroup.WithContext(r.Context())
	var (
		mu       sync.Mutex
		profiles = map[*Machine][]byte{}
		machines = p.b.Machines()
	)
	for _, m := range machines {
		if m.State() != Running {
			continue
		}
		m := m
		g.Go(func() error {
			rc, err := getProfile(ctx, m, p.which, sec, debug)
			if err != nil {
				log.Error.Printf("failed to collect profile %s from %s: %v", p.which, m.Addr, err)
				return nil
			}
			defer rc.Close()
			b, err := ioutil.ReadAll(rc)
			if err != nil {
				log.Error.Printf("failed to read profile from %s: %v", m.Addr, err)
				return nil
			}
			mu.Lock()
			profiles[m] = b
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
	// Debug output is intended for human consumption.
	if debug > 0 {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		sort.Slice(machines, func(i, j int) bool { return machines[i].Addr < machines[j].Addr })
		for _, m := range machines {
			prof := profiles[m]
			if prof == nil {
				continue
			}
			fmt.Fprintf(w, "%s:\n", m.Addr)
			w.Write(prof)
			fmt.Fprintln(w)
		}
		return
	}

	var parsed []*profile.Profile
	for m, b := range profiles {
		prof, err := profile.Parse(bytes.NewReader(b))
		if err != nil {
			log.Error.Printf("failed to parse profile from %s: %v", m.Addr, err)
			continue
		}
		parsed = append(parsed, prof)
	}
	prof, err := profile.Merge(parsed)
	if err != nil {
		profileErrorf(w, http.StatusInternalServerError, "profile merge error: %v", err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := prof.Write(w); err != nil {
		profileErrorf(w, http.StatusInternalServerError, "failed to write profile: %v", err)
	}
}

func getProfile(ctx context.Context, m *Machine, which string, sec, debug int) (rc io.ReadCloser, err error) {
	if which == "profile" {
		err = m.Call(ctx, "Supervisor.CPUProfile", time.Duration(sec)*time.Second, &rc)
	} else {
		err = m.Call(ctx, "Supervisor.Profile", profileRequest{which, debug}, &rc)
	}
	return
}

func profileErrorf(w http.ResponseWriter, code int, message string, args ...interface{}) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.WriteHeader(code)
	fmt.Fprintf(w, message, args...)
}
