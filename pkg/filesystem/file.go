package filesystem

import (
	"io"
)

// FileAppender is returned by Directory.OpenAppend(). It is a handle
// for a file that only permits new data to be written to the end.
type FileAppender interface {
	io.Closer
	io.Writer
}

// FileReader is returned by Directory.OpenRead(). It is a handle
// for a file that permits data to be read from arbitrary locations.
type FileReader interface {
	io.Closer
	io.ReaderAt
}

// FileReadWriter is returned by Directory.OpenReadWrite(). It is a
// handle for a file that permits data to be read from and written to
// arbitrary locations.
type FileReadWriter interface {
	FileReader
	FileWriter
}

// FileWriter is returned by Directory.OpenWrite(). It is a handle for a
// file that permits data to be written to arbitrary locations.
type FileWriter interface {
	io.Closer
	io.WriterAt

	Truncate(size int64) error
}
