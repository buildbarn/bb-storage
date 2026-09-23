package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type emptyBlobInjectingBlobAccess struct {
	BlobAccess[*chunk.Chunk]
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
func NewEmptyBlobInjectingBlobAccess(base BlobAccess[*chunk.Chunk]) BlobAccess[*chunk.Chunk] {
	return &emptyBlobInjectingBlobAccess{
		BlobAccess: base,
	}
}

func (ba *emptyBlobInjectingBlobAccess) Get(ctx context.Context, digest digest.Digest) (*chunk.Chunk, error) {
	if digest.GetSizeBytes() == 0 {
		emptyDigest := digest.GetDigestFunction().NewGenerator(0).Sum()
		if digest != emptyDigest {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"Empty blob has checksum %s, while %s was expected",
				emptyDigest.GetHashString(),
				digest.GetHashString(),
			)
		}
		return chunk.EmptyChunk, nil
	}
	return ba.BlobAccess.Get(ctx, digest)
}

func (ba *emptyBlobInjectingBlobAccess) Put(ctx context.Context, digest digest.Digest, value *chunk.Chunk) error {
	if digest.GetSizeBytes() == 0 {
		emptyDigest := digest.GetDigestFunction().NewGenerator(0).Sum()
		if digest != emptyDigest {
			return status.Errorf(
				codes.InvalidArgument,
				"Empty blob has checksum %s, while %s was expected",
				emptyDigest.GetHashString(),
				digest.GetHashString(),
			)
		}
		return nil
	}
	return ba.BlobAccess.Put(ctx, digest, value)
}

func (ba *emptyBlobInjectingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.BlobAccess.FindMissing(ctx, digests.RemoveEmptyBlob())
}
