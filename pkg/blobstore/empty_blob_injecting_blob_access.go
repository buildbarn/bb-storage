package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type emptyBlobInjectingBlobAccess struct {
	base BlobAccess
}

// NewEmptyBlobInjectingBlobAccess is a decorator for BlobAccess that
// causes it to directly process any requests for blobs of size zero.
// Get() operations immediately return an empty buffer, while Put()
// operations for such buffers are ignored.
//
// Bazel never attempts to read the empty blob from the Content
// Addressable Storage, which by itself is harmless. In addition to
// that, it never attempts to write the empty blob. This is problematic,
// as it may cause unaware implementations of GetActionResult() and
// input root population to fail.
//
// This problem remained undetected for a long time, because running at
// least one build action through bb_worker has a high probability of
// creating the empty blob in storage explicitly.
//
// The consensus within the Remote APIs working group has been to give
// the empty blob a special meaning: the system must behave as if this
// blob is always present.
//
// More details: https://github.com/bazelbuild/bazel/issues/11063
func NewEmptyBlobInjectingBlobAccess(base BlobAccess) BlobAccess {
	return &emptyBlobInjectingBlobAccess{
		base: base,
	}
}

func (ba *emptyBlobInjectingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	if digest.GetSizeBytes() == 0 {
		return buffer.NewCASBufferFromByteSlice(digest, nil, buffer.UserProvided)
	}
	return ba.base.Get(ctx, digest)
}

func (ba *emptyBlobInjectingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	if digest.GetSizeBytes() == 0 {
		_, err := b.ToByteSlice(0)
		return err
	}
	return ba.base.Put(ctx, digest, b)
}

func (ba *emptyBlobInjectingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.base.FindMissing(ctx, digests.RemoveEmptyBlob())
}
