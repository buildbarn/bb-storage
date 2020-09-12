package blobstore

import (
	"io"
	"math"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
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
	// NewBufferFromReaderAt creates a buffer from a reader that
	// provides random access.
	NewBufferFromReaderAt(digest digest.Digest, r buffer.ReadAtCloser, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
}

func newReaderFromReaderAt(r buffer.ReadAtCloser) io.ReadCloser {
	return &struct {
		io.SectionReader
		io.Closer
	}{
		SectionReader: *io.NewSectionReader(r, 0, math.MaxInt64),
		Closer:        r,
	}
}
