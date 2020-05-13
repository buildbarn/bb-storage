package blockdevice

import (
	"io"
)

// ReadWriterAt is an interface for reading from/writing to a byte slice
// like data source (e.g., a file or disk).
type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}
