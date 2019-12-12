package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type casStorageType struct{}

func (f casStorageType) GetDigestKey(digest *util.Digest) string {
	return digest.GetKey(util.DigestKeyWithoutInstance)
}

func (f casStorageType) NewBufferFromByteSlice(digest *util.Digest, data []byte, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewCASBufferFromByteSlice(digest, data, repairStrategy)
}

func (f casStorageType) NewBufferFromReader(digest *util.Digest, r io.ReadCloser, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewCASBufferFromReader(digest, r, repairStrategy)
}

// CASStorageType is capable of creating identifiers and buffers for
// objects stored in the Content Addressable Storage (CAS).
var CASStorageType StorageType = casStorageType{}
