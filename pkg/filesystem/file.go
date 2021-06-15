package filesystem

import (
	"io"
)

// RegionType is an enumeration type that can be provided to
// FileReader.GetNextRegionOffset() to specify whether the next offset
// for data or a hole should be obtained.
type RegionType int

const (
	// Data region inside a file.
	Data RegionType = 1
	// Hole inside a sparse file.
	Hole RegionType = 2
)

// FileAppender is returned by Directory.OpenAppend(). It is a handle
// for a file that only permits new data to be written to the end.
type FileAppender interface {
	io.Closer
	io.Writer

	Sync() error
}

// FileReader is returned by Directory.OpenRead(). It is a handle
// for a file that permits data to be read from arbitrary locations.
type FileReader interface {
	io.Closer
	io.ReaderAt

	// Equivalent to lseek() with SEEK_DATA and SEEK_HOLE.
	//
	// These functions return io.EOF when the provided offset points
	// to or past the end-of-file position. Calling this function
	// with Data may also return io.EOF when no more data regions
	// exist past the provided offset.
	GetNextRegionOffset(offset int64, regionType RegionType) (int64, error)
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

	Sync() error
	Truncate(size int64) error
}
