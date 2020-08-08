package blobstore

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/protobuf/proto"
)

type casStorageType struct{}

func (f casStorageType) GetDigestKey(blobDigest digest.Digest) string {
	// TODO: Now that we have DemultiplexingBlobAccess, this
	// assumption is no longer correct. When multiple backends are
	// used, the instance name must be retained to ensure requests
	// are routed properly.
	//
	// We should phase out GetDigestKey() and automatically
	// determine whether KeyWithInstance or KeyWithoutInstance needs
	// to be used based on the hierarchy created through
	// NewBlobAccessFromConfiguration().
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

// CASPutProto is a helper function for storing Protobuf messages in the
// Content Addressable Storage (CAS). It computes the digest of the
// message and stores it under that key. The digest is then returned, so
// that the object may be referenced.
func CASPutProto(ctx context.Context, blobAccess BlobAccess, message proto.Message, parentDigest digest.Digest) (digest.Digest, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}

	// Compute new digest of data.
	digestGenerator := parentDigest.NewGenerator()
	if _, err := digestGenerator.Write(data); err != nil {
		panic(err)
	}
	blobDigest := digestGenerator.Sum()

	if err := blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(data)); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}
