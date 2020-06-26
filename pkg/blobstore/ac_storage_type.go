package blobstore

import (
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type acStorageType struct{}

func (f acStorageType) GetDigestKey(blobDigest digest.Digest) string {
	return blobDigest.GetKey(digest.KeyWithInstance)
}

func (f acStorageType) NewBufferFromByteSlice(digest digest.Digest, data []byte, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewProtoBufferFromByteSlice(&remoteexecution.ActionResult{}, data, repairStrategy)
}

func (f acStorageType) NewBufferFromReader(digest digest.Digest, r io.ReadCloser, repairStrategy buffer.RepairStrategy) buffer.Buffer {
	return buffer.NewProtoBufferFromReader(&remoteexecution.ActionResult{}, r, repairStrategy)
}

// ACStorageType is capable of creating identifiers and buffers for
// objects stored in the Action Cache (AC).
var ACStorageType StorageType = acStorageType{}
