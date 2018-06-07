package rpc

import (
	"io"
	"math/rand"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/log"
)

// InjectFailures causes HTTP responses to be randomly terminated.  Only for
// unittesting.
var InjectFailures = false

// rpcFaultInjector is an io.ReadCloser implementation that wraps another
// io.ReadCloser but inject artificial failures.
type rpcFaultInjector struct {
	label string // for logging.
	in    io.ReadCloser
	err   error
}

func (r *rpcFaultInjector) error(err error) {
	r.in.Close()
	r.in = nil
	r.err = err
}

// Read implements io.Reader.
func (r *rpcFaultInjector) Read(buf []byte) (int, error) {
	if r.in == nil {
		return 0, r.err
	}
	x := rand.Float32()
	if x < 0.005 {
		r.error(errors.E(errors.Net, errors.Retriable, r.label+": test-induced message drop"))
		log.Error.Printf("faultinjector %s: dropping message", r.label)
		return 0, r.err
	}
	n, err := r.in.Read(buf)
	if x < 0.01 {
		nn := int(float64(n) * rand.Float64())
		log.Error.Printf("faultinjector %s: truncating message from %d->%d", r.label, n, nn)
		n = nn
		err = errors.E(errors.Net, errors.Retriable, r.label+": test-induced message truncation")
		r.error(err)
	}
	return n, err
}

// Close implements io.Closer.
func (r *rpcFaultInjector) Close() error {
	if r.in != nil {
		return r.in.Close()
	}
	return nil
}
