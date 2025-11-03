package buffer

import (
	"io"
)

type errorReader struct {
	err error
}

// newErrorReader creates an io.ReadCloser that always returns a fixed
// error response for Read() operations.
func newErrorReader(err error) io.ReadCloser {
	return errorReader{err: err}
}

func (r errorReader) Read(p []byte) (int, error) {
	return 0, r.err
}

func (errorReader) Close() error {
	return nil
}
