package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// StorageType is passed to many implementations of BlobAccess to be
// able to use the same BlobAccess implementation for both the Content
// Addressable Storage (CAS) and Action Cache (AC). This interface
// provides functionns for generic object keying and buffer creation.
type StorageType interface {
	// GetDigestKey creates a string key that may be used as an
	// identifier for a blob. This function is, for example, used to
	// determine the name of keys in S3 and Redis.
	GetDigestKey(digest digest.Digest) string

	// NewBufferFromByteSlice creates a buffer from a byte slice
	// that is either suitable for storage in the CAS or AC.
	NewBufferFromByteSlice(digest digest.Digest, data []byte, repairStrategy buffer.RepairStrategy) buffer.Buffer
	// NewBufferFromByteSlice creates a buffer from a reader that is
	// either suitable for storage in the CAS or AC.
	NewBufferFromReader(digest digest.Digest, r io.ReadCloser, repairStrategy buffer.RepairStrategy) buffer.Buffer
}
