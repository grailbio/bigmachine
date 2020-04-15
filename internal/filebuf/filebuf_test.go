package filebuf

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/grailbio/testutil/expect"
)

type fakeReadCloser struct {
	io.Reader
	closed bool
}

func (r *fakeReadCloser) Close() error {
	r.closed = true
	return nil
}

type errorReader struct {
	err error
}

func (r errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

// TestFileBuf verifies that we can create, read from, and close a FileBuf.
func TestFileBuf(t *testing.T) {
	in := make([]byte, 1<<20)
	rand.Read(in)
	rc := &fakeReadCloser{
		Reader: bytes.NewReader(append([]byte{}, in...)),
	}
	b, err := New(rc)
	expect.NoError(t, err)
	out, err := ioutil.ReadAll(b)
	expect.NoError(t, err)
	expect.EQ(t, out, in)
	err = b.Close()
	expect.NoError(t, err)
	expect.True(t, rc.closed)
}

// TestFileBufReadError verifies that an error reading from the underlying
// reader is propagated.
func TestFileBufReadError(t *testing.T) {
	r := errorReader{fmt.Errorf("test error")}
	_, err := New(r)
	expect.HasSubstr(t, err.Error(), "test error")
}
