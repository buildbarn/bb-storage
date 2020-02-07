package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type casStorageType struct{}

func (f casStorageType) GetDigestKey(blobDigest digest.Digest) string {
	return blobDigest.GetKey(digest.KeyWithoutInstance)
}

func (f casStorageType) NewBufferFromByteSlice(digest digest.Digest, data []byte, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewCASBufferFromByteSlice(digest, data, repairStrategy)
}

func (f casStorageType) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewCASBufferFromReader(digest, r, repairStrategy)
}

// CASStorageType is capable of creating identifiers and buffers for
// objects stored in the Content Addressable Storage (CAS).
var CASStorageType StorageType = casStorageType{}
