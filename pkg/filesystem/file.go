package filesystem

import (
	"io"
)

// File is an interface for the operations that are applied on regular
// files opened through Directory.OpenFile().
type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Writer
	io.WriterAt
}
