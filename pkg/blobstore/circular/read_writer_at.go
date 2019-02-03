package circular

import (
	"io"
)

// ReadWriterAt is an interface for the file operations performed by the
// circular storage layer.
type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}
