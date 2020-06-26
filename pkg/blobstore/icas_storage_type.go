package blobstore

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
)

type icasStorageType struct{}

func (f icasStorageType) GetDigestKey(blobDigest digest.Digest) string {
	return blobDigest.GetKey(digest.KeyWithoutInstance)
}

func (f icasStorageType) NewBufferFromByteSlice(digest digest.Digest, data []byte, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&icas.Reference{}, data, repairStrategy)
}

func (f icasStorageType) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&icas.Reference{}, r, repairStrategy)
}

// ICASStorageType is capable of creating identifiers and buffers for
// objects stored in the Indirect Content Addressable Storage (ICAS).
var ICASStorageType StorageType = icasStorageType{}
