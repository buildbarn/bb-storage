package buffer

import "io"

// ReadAtCloser is an interface that combines io.ReaderAt and io.Closer.
type ReadAtCloser interface {
	io.ReaderAt
	io.Closer
}
