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
	"github.com/grailbio/base/diagnostic/dump"
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

func (h *profileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		sec64, _ = strconv.ParseInt(r.FormValue("seconds"), 10, 64)
		sec      = int(sec64)
		debug, _ = strconv.Atoi(r.FormValue("debug"))
		gc, _    = strconv.Atoi(r.FormValue("gc"))
		addr     = r.FormValue("machine")
	)
	p := profiler{
		b:     h.b,
		which: h.which,
		addr:  addr,
		sec:   sec,
		debug: debug,
		gc:    gc > 0,
	}
	w.Header().Set("Content-Type", p.ContentType())
	err := p.Marshal(r.Context(), w)
	if err != nil {
		code := http.StatusInternalServerError
		if _, ok := err.(errNoProfiles); ok {
			code = http.StatusNotFound
		}
		profileErrorf(w, code, err.Error())
	}
}

func getProfile(ctx context.Context, m *Machine, which string, sec, debug int, gc bool) (rc io.ReadCloser, err error) {
	if which == "profile" {
		err = m.Call(ctx, "Supervisor.CPUProfile", time.Duration(sec)*time.Second, &rc)
	} else {
		err = m.Call(ctx, "Supervisor.Profile", profileRequest{which, debug, gc}, &rc)
	}
	return
}

func profileErrorf(w http.ResponseWriter, code int, message string, args ...interface{}) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.WriteHeader(code)
	if _, err := fmt.Fprintf(w, message, args...); err != nil {
		log.Printf("error writing profile 500: %v", err)
	}
}

// profiler writes (possibly aggregated) profiles, configured by its fields.
type profiler struct {
	b *B
	// which is the name of the profile to write.
	which string
	// addr is the specific machine's profile to write. If addr == "", profiles
	// are aggregated from all of b's machines.
	addr string
	// sec is the number of seconds for which to generate a CPU profile. It is
	// only relevant when which == "profile".
	sec int
	// debug is the debug value passed to pprof to determine the format of the
	// profile output. See pprof documentation for details.
	debug int
	// gc determines whether we request a garbage collection before taking the
	// profile. This is only relevant when which == "heap".
	gc bool
}

// errNoProfiles is a marker type for the error that is returned by
// (profiler).Marshal when there are no profiles from the cluster machines. We
// use this to signal that we want to return a StatusNotFound when we are
// writing the profile in an HTTP response.
type errNoProfiles string

func (e errNoProfiles) Error() string {
	return string(e)
}

// ContentType returns the expected content type, assuming success, of a call
// to Marshal. This is used to set the Content-Type header when we are writing
// the profile in an HTTP response. This may be overridden if there is an
// error.
func (p profiler) ContentType() string {
	if p.debug > 0 && p.which != "profile" {
		return "text/plain; charset=utf-8"
	}
	return "application/octet-stream"
}

// Marshal writes the profile configured in pw to w.
func (p profiler) Marshal(ctx context.Context, w io.Writer) (err error) {
	if p.addr != "" {
		m, err := p.b.Dial(ctx, p.addr)
		if err != nil {
			return fmt.Errorf("failed to dial machine: %v", err)
		}
		rc, err := getProfile(ctx, m, p.which, p.sec, p.debug, p.gc)
		if err != nil {
			return fmt.Errorf("failed to collect %s profile: %v", p.which, err)
		}
		defer func() {
			cerr := rc.Close()
			if err == nil {
				err = cerr
			}
		}()
		_, err = io.Copy(w, rc)
		if err != nil {
			return fmt.Errorf("failed to write %s profile: %v", p.which, err)
		}
		return nil
	}
	g, ctx := errgroup.WithContext(ctx)
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
		g.Go(func() (err error) {
			rc, err := getProfile(ctx, m, p.which, p.sec, p.debug, p.gc)
			if err != nil {
				log.Error.Printf("failed to collect profile %s from %s: %v", p.which, m.Addr, err)
				return nil
			}
			defer func() {
				cerr := rc.Close()
				if err == nil {
					err = cerr
				}
			}()
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
		return fmt.Errorf("failed to fetch profiles: %v", err)
	}
	if len(profiles) == 0 {
		return errNoProfiles("no profiles are available at this time")
	}
	// Debug output is intended for human consumption.
	if p.debug > 0 && p.which != "profiles" {
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
		return nil
	}

	var parsed []*profile.Profile
	for m, b := range profiles {
		prof, err := profile.Parse(bytes.NewReader(b))
		if err != nil {
			return fmt.Errorf("failed to parse profile from %s: %v", m.Addr, err)
		}
		parsed = append(parsed, prof)
	}
	prof, err := profile.Merge(parsed)
	if err != nil {
		return fmt.Errorf("profile merge error: %v", err)
	}
	if err := prof.Write(w); err != nil {
		return fmt.Errorf("failed to write profile: %v", err)
	}
	return nil
}

func makeProfileDumpFunc(b *B, which string, debug int) dump.Func {
	p := profiler{
		b:     b,
		which: which,
		sec:   30,
		debug: debug,
		gc:    false,
	}
	return func(ctx context.Context, w io.Writer) error {
		return p.Marshal(ctx, w)
	}
}
