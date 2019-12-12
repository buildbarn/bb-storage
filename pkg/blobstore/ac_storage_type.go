package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type acStorageType struct{}

func (f acStorageType) GetDigestKey(digest *util.Digest) string {
	return digest.GetKey(util.DigestKeyWithInstance)
}

func (f acStorageType) NewBufferFromByteSlice(digest *util.Digest, data []byte, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewACBufferFromByteSlice(data, repairStrategy)
}

func (f acStorageType) NewBufferFromReader(digest *util.Digest, r io.ReadCloser, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewACBufferFromReader(r, repairStrategy)
}

// ACStorageType is capable of creating identifiers and buffers for
// objects stored in the Action Cache (AC).
var ACStorageType StorageType = acStorageType{}
