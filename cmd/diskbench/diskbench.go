package main

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strings"

	"github.com/grailbio/base/config"
	"github.com/grailbio/base/grail"
	"github.com/grailbio/base/log"
	"github.com/grailbio/base/must"
	"github.com/grailbio/bigmachine"
	_ "github.com/grailbio/bigmachine/ec2system"
	"github.com/grailbio/bigmachine/rpc"
)

func main() {
	bigmachine.Init()
	grail.Init()
	var sys bigmachine.System
	must.Nil(config.Instance("bigmachine", &sys))
	b := bigmachine.Start(sys)
	defer b.Shutdown()
	ctx := context.Background()
	machines, err := b.Start(ctx, 1, bigmachine.Services{"Bench": bench{}})
	must.Nil(err, "starting machines")
	log.Print("waiting for machine")
	m := machines[0]
	<-m.Wait(bigmachine.Running)
	log.Print("running benchmark")
	var rc io.ReadCloser
	must.Nil(m.Call(ctx, "Bench.Run", struct{}{}, &rc))
	defer func() {
		must.Nil(rc.Close())
	}()
	_, err = io.Copy(os.Stdout, rc)
	must.Nil(err)
}

func init() {
	gob.Register(bench{})
}

type bench struct{}

func (bench) Run(ctx context.Context, _ struct{}, rc *io.ReadCloser) error {
	r, w := io.Pipe()
	*rc = rpc.Flush(r)
	go func() {
		if err := run(w); err != nil {
			if closeErr := w.CloseWithError(err); closeErr != nil {
				log.Error.Printf("closing pipe writer: %v", closeErr)
			}
			return
		}
		if err := w.Close(); err != nil {
			log.Printf("closing pipe writer: %v", err)
		}
	}()
	return nil
}

func run(w io.Writer) error {
	var (
		msg    string
		tmpDir = os.Getenv("TMPDIR")
	)
	if tmpDir == "" {
		msg = "$TMPDIR empty; assuming /tmp"
		tmpDir = "/tmp"
	} else {
		msg = fmt.Sprintf("$TMPDIR is %s\n", tmpDir)
	}
	if _, err := io.WriteString(w, msg); err != nil {
		return fmt.Errorf("writing $TMPDIR value: %v", err)
	}
	dev, err := resolveDev(tmpDir)
	if err != nil {
		return fmt.Errorf("resolving device of %s: %v", tmpDir, err)
	}
	const N = 3
	for i := 0; i < N; i++ {
		status := fmt.Sprintf("===\n=== benchmark run %d of %d\n===\n", i+1, N)
		if _, err = io.WriteString(w, status); err != nil {
			return fmt.Errorf("writing status: %v", err)
		}
		if err := dd(w, tmpDir); err != nil {
			return fmt.Errorf("running dd: %v", err)
		}
		if err := hdparm(w, dev); err != nil {
			return fmt.Errorf("running hdparm: %v", err)
		}
	}
	return nil
}

func resolveDev(p string) (string, error) {
	cmd := exec.Command("findmnt", "-n", "-o", "SOURCE", "--target", p)
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("running findmnt to resolve %s: %v", p, err)
	}
	return strings.TrimRight(string(out), "\n"), nil
}

func dd(w io.Writer, tmpDir string) error {
	if _, err := io.WriteString(w, "= writing and reading with dd\n"); err != nil {
		return fmt.Errorf("writing status: %v", err)
	}
	p := path.Join(tmpDir, "bench.tmp")
	if err := runCmd(w, "dd",
		"if=/dev/zero",
		fmt.Sprintf("of=%s", p),
		"conv=fdatasync",
		"bs=1M",
		"count=1024",
	); err != nil {
		return fmt.Errorf("writing with dd: %v", err)
	}
	if err := runCmd(w, "dd",
		fmt.Sprintf("if=%s", p),
		"of=/dev/null",
		"bs=1M",
		"count=1024",
	); err != nil {
		return fmt.Errorf("reading with dd: %v", err)
	}
	return nil
}

func hdparm(w io.Writer, dev string) error {
	if _, err := io.WriteString(w, "= running hdparm -Tt\n"); err != nil {
		return fmt.Errorf("writing status: %v", err)
	}
	return runCmd(w, "hdparm", "-Tt", dev)
}

func runCmd(w io.Writer, name string, arg ...string) error {
	cmd := exec.Command(name, arg...)
	cmd.Stdout = w
	cmd.Stderr = w
	return cmd.Run()
}
