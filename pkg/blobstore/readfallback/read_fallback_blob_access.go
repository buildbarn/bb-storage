package readfallback

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readFallbackBlobAccess struct {
	blobstore.BlobAccess
	secondary  blobstore.BlobAccess
	replicator replication.BlobReplicator
}

// NewReadFallbackBlobAccess creates a decorator for BlobAccess that
// causes reads for non-existent to be forwarded to a secondary storage
// backend. Data is never written to the latter.
//
// This decorator can be used to integrate external data sets into the
// system, e.g. by combining it with ReferenceExpandingBlobAccess.
func NewReadFallbackBlobAccess(primary, secondary blobstore.BlobAccess, replicator replication.BlobReplicator) blobstore.BlobAccess {
	return &readFallbackBlobAccess{
		BlobAccess: primary,
		secondary:  secondary,
		replicator: replicator,
	}
}

func (ba *readFallbackBlobAccess) getBlobReplicatorSelector() replication.BlobReplicatorSelector {
	replicator := ba.replicator
	return func(observedErr error) (replication.BlobReplicator, error) {
		if status.Code(observedErr) != codes.NotFound {
			// One of the backends returned an error other than
			// NOT_FOUND. Prepend the name of the backend to make
			// debugging easier.
			if replicator != nil {
				return nil, util.StatusWrap(observedErr, "Primary")
			}
			return nil, util.StatusWrap(observedErr, "Secondary")
		}
		if replicator == nil {
			// We already tried the secondary below and got another
			// codes.NotFound, so just return that error.
			return nil, observedErr
		}

		replicatorToReturn := replicator
		replicator = nil
		return replicatorToReturn, nil
	}
}

func (ba *readFallbackBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return replication.GetWithBlobReplicator(
		ctx,
		digest,
		ba.BlobAccess,
		ba.getBlobReplicatorSelector())
}

func (ba *readFallbackBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return replication.GetFromCompositeWithBlobReplicator(
		ctx,
		parentDigest,
		childDigest,
		slicer,
		ba.BlobAccess,
		ba.getBlobReplicatorSelector())
}

func (ba *readFallbackBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
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
