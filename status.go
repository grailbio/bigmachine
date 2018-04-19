// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package bigmachine

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/grailbio/base/data"
	"golang.org/x/sync/errgroup"
)

var startTime = time.Now()

var statusTemplate = template.Must(template.New("status").
	Funcs(template.FuncMap{
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
	memory:
		total:	{{human .mem.System.Total}}
		used:	{{human .mem.System.Used}}
		(percent):	{{printf "%.1f%%" .mem.System.UsedPercent}}
		available:	{{human .mem.System.Available}}
		runtime:	{{human .mem.Runtime.Sys}}
	runtime:
		uptime:	{{.uptime}}
		pausetime:	{{ns .mem.Runtime.PauseTotalNs}}
		(last):	{{ns .lastpause}}
	disk:
		total:	{{human .disk.Usage.Total}}
		available:	{{human .disk.Usage.Free}}
		used:	{{human .disk.Usage.Used}}
		(percent):	{{printf "%.1f%%" .disk.Usage.UsedPercent}}
	load: {{printf "%.1f %.1f %.1f" .load.Averages.Load1 .load.Averages.Load5 .load.Averages.Load15}}
`))

// StatusHandler implements an HTTP handler that displays machine
// statuses.
type statusHandler struct{ *B }

func (s *statusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	machines := s.B.Machines()
	sort.Slice(machines, func(i, j int) bool {
		return machines[i].Addr < machines[j].Addr
	})
	infos := make([]machineInfo, len(machines))
	g, ctx := errgroup.WithContext(r.Context())
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
		http.Error(w, fmt.Sprint(err), 500)
		return
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
			"mem":       info.MemInfo,
			"disk":      info.DiskInfo,
			"load":      info.LoadInfo,
			"uptime":    time.Since(startTime),
			"lastpause": info.MemInfo.Runtime.PauseNs[(info.MemInfo.Runtime.NumGC+255)%256],
		})
		if err != nil {
			panic(err)
		}
	}
}

type machineInfo struct {
	err error
	MemInfo
	DiskInfo
	LoadInfo
}

func allInfo(ctx context.Context, m *Machine) machineInfo {
	g, ctx := errgroup.WithContext(ctx)
	var (
		mem  MemInfo
		disk DiskInfo
		load LoadInfo
	)
	g.Go(func() error {
		var err error
		mem, err = m.MemInfo(ctx)
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
	return machineInfo{err, mem, disk, load}
}
