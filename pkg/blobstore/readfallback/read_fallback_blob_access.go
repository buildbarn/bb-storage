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
	secondary             blobstore.BlobAccess[T]
	getReplicator         replication.BlobReplicator[T]
	findMissingReplicator replication.BlobReplicator[T]
}

// NewReadFallbackBlobAccess creates a decorator for BlobAccess that
// causes reads for non-existent to be forwarded to a secondary storage
// backend. Data is only ever written to the primary storage backend.
//
// Objects that are only present in the secondary backend are copied
// to the primary backend using getReplicator during reads, and using
// findMissingReplicator during FindMissing() calls. Both arguments may
// refer to the same replicator, so that concurrency limits and
// deduplication are shared across both kinds of calls.
func NewReadFallbackBlobAccess[T any](primary, secondary blobstore.BlobAccess[T], getReplicator, findMissingReplicator replication.BlobReplicator[T]) blobstore.BlobAccess[T] {
	return &readFallbackBlobAccess[T]{
		BlobAccess:            primary,
		secondary:             secondary,
		getReplicator:         getReplicator,
		findMissingReplicator: findMissingReplicator,
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
	ret, err = ba.getReplicator.ReplicateSingle(ctx, digest)
	if err != nil && status.Code(err) != codes.NotFound {
		return zero, util.StatusWrap(err, "Secondary")
	}
	return ret, err
}

func (ba *readFallbackBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	missingInPrimary, err := ba.BlobAccess.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Primary")
	}
	missingInBoth, err := ba.secondary.FindMissing(ctx, missingInPrimary)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Secondary")
	}

	presentOnlyInSecondary, _, _ := digest.GetDifferenceAndIntersection(missingInPrimary, missingInBoth)
	if err := ba.findMissingReplicator.ReplicateMultiple(ctx, presentOnlyInSecondary); err != nil {
		if status.Code(err) == codes.NotFound {
			return digest.EmptySet, util.StatusWrapWithCode(err, codes.Internal, "Backend secondary returned inconsistent results while synchronizing")
		}
		return digest.EmptySet, util.StatusWrap(err, "Failed to synchronize from backend secondary to backend primary")
	}

	return missingInBoth, nil
}
