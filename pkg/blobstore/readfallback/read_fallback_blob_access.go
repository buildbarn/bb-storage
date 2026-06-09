package readfallback

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readFallbackBlobAccess[T any] struct {
	blobstore.BlobAccess[T]
	secondary  blobstore.BlobAccess[T]
	replicator replication.BlobReplicator
}

// NewReadFallbackBlobAccess creates a decorator for BlobAccess that
// causes reads for non-existent to be forwarded to a secondary storage
// backend. Data is never written to the latter.
//
// This decorator can be used to integrate external data sets into the
// system, e.g. by combining it with ReferenceExpandingBlobAccess.
func NewReadFallbackBlobAccess[T any](primary, secondary blobstore.BlobAccess[T], replicator replication.BlobReplicator) blobstore.BlobAccess[T] {
	return &readFallbackBlobAccess[T]{
		BlobAccess: primary,
		secondary:  secondary,
		replicator: replicator,
	}
}

func (ba *readFallbackBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	var zero T
	ret, err := ba.BlobAccess.Get(ctx, digest)
	if err == nil {
		return ret, nil
	}
	if status.Code(err) != codes.NotFound {
		return zero, util.StatusWrap(err, "Primary")
	}
	err = ba.replicator.ReplicateMultiple(ctx, digest.ToSingletonSet())
	if err != nil && status.Code(err) != codes.NotFound {
		return zero, util.StatusWrap(err, "Secondary")
	}

	return ba.BlobAccess.Get(ctx, digest)
}

func (ba *readFallbackBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on the backends sequentially, as opposed
	// to calling them concurrently and merging the results. In the
	// common case, the primary backend is capable of pruning most
	// of the digests, making the call to the secondary backend a
	// lot smaller.
	missingInPrimary, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Primary")
	}
	missingInBoth, err := ba.secondary.FindMissing(ctx, missingInPrimary)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Secondary")
	}

	// Replicate the blobs that are present only in the secondary
	// backend to the primary backend.
	presentOnlyInSecondary, _, _ := digest.GetDifferenceAndIntersection(missingInPrimary, missingInBoth)
	if err := ba.replicator.ReplicateMultiple(ctx, presentOnlyInSecondary); err != nil {
		if status.Code(err) == codes.NotFound {
			return digest.EmptySet, util.StatusWrapWithCode(err, codes.Internal, "Backend secondary returned inconsistent results while synchronizing")
		}
		return digest.EmptySet, util.StatusWrap(err, "Failed to synchronize from backend secondary to backend primary")
	}

	return missingInBoth, nil
}
