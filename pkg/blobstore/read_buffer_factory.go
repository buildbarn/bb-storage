package blobstore

import (
	"io"
	"math"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

// ReadBufferFactory is passed to many implementations of BlobAccess to
// be able to use the same BlobAccess implementation for both the
// Content Addressable Storage (CAS), Action Cache (AC) and Indirect
// Content Addressable Storage (ICAS). This interface provides functions
// for buffer creation.
type ReadBufferFactory interface {
	// NewBufferFromByteSlice creates a buffer from a byte slice.
	NewBufferFromByteSlice(digest digest.Digest, data []byte, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
	// NewBufferFromReader creates a buffer from a reader.
	NewBufferFromReader(digest digest.Digest, r io.ReadCloser, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
	// NewBufferFromFileReader creates a buffer from a reader that
	// provides random access.
	NewBufferFromFileReader(digest digest.Digest, r filesystem.FileReader, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
}

func newReaderFromFileReader(r filesystem.FileReader) io.ReadCloser {
	return &struct {
		io.SectionReader
		io.Closer
	}{
		SectionReader: *io.NewSectionReader(r, 0, math.MaxInt64),
		Closer:        r,
	}
}
