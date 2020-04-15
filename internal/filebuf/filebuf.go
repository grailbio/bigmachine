package filebuf

import (
	"io"
	"io/ioutil"
	"os"

	"github.com/grailbio/base/errors"
)

// FileBuf is a file-backed buffer that buffers the entire contents of reader.
// This is useful for fully buffering a network read with low memory overhead.
type FileBuf struct {
	// file is the temporary file that backs this buffer.
	file *os.File
}

// New creates a new file-backed buffer containing the entire contents of r. If
// there is an error reading from r, an error is returned. If r is an io.Closer,
// r is closed once it is fully read.
func New(r io.Reader) (b *FileBuf, err error) {
	if rc, ok := r.(io.Closer); ok {
		defer rc.Close()
	}
	file, err := ioutil.TempFile("", "bigmachine-filebuf-")
	if err != nil {
		return nil, errors.E("error opening temp file for filebuf", err)
	}
	defer func() {
		if err != nil {
			// We created the temporary file but had some other downstream
			// error, so we clean up the file now instead of in Close.
			os.Remove(file.Name())
		}
	}()
	_, err = io.Copy(file, r)
	if err != nil {
		return nil, errors.E("error reading into filebuf", err)
	}
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, errors.E("error seeking in filebuf", err)
	}
	return &FileBuf{file: file}, nil
}

// Read implements (io.Reader).Read.
func (b *FileBuf) Read(p []byte) (int, error) {
	return b.file.Read(p)
}

// Close implements (io.Closer).Close.
func (b *FileBuf) Close() error {
	if b.file == nil {
		return nil
	}
	defer os.Remove(b.file.Name())
	err := b.file.Close()
	b.file = nil
	return err
}
