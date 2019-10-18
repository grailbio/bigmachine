// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/base/diagnostic/dump"
	"golang.org/x/sync/errgroup"
)

var startTime = time.Now()

var statusTemplate = template.Must(template.New("status").
	Funcs(template.FuncMap{
		"roundjoindur": func(ds []time.Duration) string {
			strs := make([]string, len(ds))
			for i, d := range ds {
				d = d - d%time.Millisecond
				strs[i] = d.String()
			}
			return strings.Join(strs, " ")
		},
		"until": time.Until,
		"human": func(v interface{}) string {
			switch v := v.(type) {
			case int:
				return data.Size(v).String()
			case int64:
				return data.Size(v).String()
			case uint64:
				return data.Size(v).String()
			default:
				return fmt.Sprintf("(!%T)%v", v, v)
			}
		},
		"ns": func(v interface{}) string {
			switch v := v.(type) {
			case int:
				return time.Duration(v).String()
			case int64:
				return time.Duration(v).String()
			case uint64:
				return time.Duration(v).String()
			default:
				return fmt.Sprintf("(!%T)%v", v, v)
			}
		},
	}).
	Parse(`{{.machine.Addr}}
{{if .machine.Owned}}	keepalive:
		next:	{{.info.NextKeepalive}} (in {{until .info.NextKeepalive}})
		reply times:	{{roundjoindur .info.KeepaliveReplyTimes}}
{{end}}	memory:
		total:	{{human .info.MemInfo.System.Total}}
		used:	{{human .info.MemInfo.System.Used}}
		(percent):	{{printf "%.1f%%" .info.MemInfo.System.UsedPercent}}
		available:	{{human .info.MemInfo.System.Available}}
		runtime:	{{human .info.MemInfo.Runtime.Sys}}
		(alloc):	{{human .info.MemInfo.Runtime.Alloc}}
	runtime:
		uptime:	{{.uptime}}
		pausetime:	{{ns .info.MemInfo.Runtime.PauseTotalNs}}
		(last):	{{ns .lastpause}}
	disk:
		total:	{{human .info.DiskInfo.Usage.Total}}
		available:	{{human .info.DiskInfo.Usage.Free}}
		used:	{{human .info.DiskInfo.Usage.Used}}
		(percent):	{{printf "%.1f%%" .info.DiskInfo.Usage.UsedPercent}}
	load: {{printf "%.1f %.1f %.1f" .info.LoadInfo.Averages.Load1 .info.LoadInfo.Averages.Load5 .info.LoadInfo.Averages.Load15}}
`))

func makeStatusDumpFunc(b *B) dump.Func {
	return func(ctx context.Context, w io.Writer) error {
		return writeStatus(ctx, b, w)
	}
}

func writeStatus(ctx context.Context, b *B, w io.Writer) error {
	machines := b.Machines()
	sort.Slice(machines, func(i, j int) bool {
		return machines[i].Addr < machines[j].Addr
	})
	infos := make([]machineInfo, len(machines))
	g, ctx := errgroup.WithContext(ctx)
	for i, m := range machines {
		if state := m.State(); state != Running {
			infos[i].err = fmt.Errorf("machine state %s", state)
			continue
		}
		i, m := i, m
		g.Go(func() error {
			infos[i] = allInfo(ctx, m)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	var tw tabwriter.Writer
	tw.Init(w, 4, 4, 1, ' ', 0)
	defer tw.Flush()
	for i, info := range infos {
		m := machines[i]
		if info.err != nil {
			fmt.Fprintln(&tw, m.Addr, ":", info.err)
			continue
		}
		err := statusTemplate.Execute(&tw, map[string]interface{}{
			"machine":   m,
			"info":      info,
			"uptime":    time.Since(startTime),
			"lastpause": info.MemInfo.Runtime.PauseNs[(info.MemInfo.Runtime.NumGC+255)%256],
		})
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// StatusHandler implements an HTTP handler that displays machine
// statuses.
type statusHandler struct{ *B }

func (s *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err := writeStatus(r.Context(), s.B, w); err != nil {
		http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
	}
}

type machineInfo struct {
	err error
	MemInfo
	DiskInfo
	LoadInfo
	KeepaliveReplyTimes []time.Duration
	NextKeepalive       time.Time
}

func allInfo(ctx context.Context, m *Machine) machineInfo {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	var (
		mem  MemInfo
		disk DiskInfo
		load LoadInfo
	)
	g.Go(func() error {
		var err error
		mem, err = m.MemInfo(ctx, true)
		return err
	})
	g.Go(func() error {
		var err error
		disk, err = m.DiskInfo(ctx)
		return err
	})
	g.Go(func() error {
		var err error
		load, err = m.LoadInfo(ctx)
		return err
	})
	err := g.Wait()
	return machineInfo{
		err:                 err,
		MemInfo:             mem,
		DiskInfo:            disk,
		LoadInfo:            load,
		KeepaliveReplyTimes: m.KeepaliveReplyTimes(),
		NextKeepalive:       m.NextKeepalive(),
	}
}
