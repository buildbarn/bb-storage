package readfallback

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readFallbackBlobAccess struct {
	primary    blobstore.BlobAccess
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
		primary:    primary,
		secondary:  secondary,
		replicator: replicator,
	}
}

func (ba *readFallbackBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.primary.Get(ctx, digest),
		&readFallbackErrorHandler{
			replicator: ba.replicator,
			context:    ctx,
			digest:     digest,
		})
}

func (ba *readFallbackBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return ba.primary.Put(ctx, digest, b)
}

func (ba *readFallbackBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Call FindMissing() on the backends sequentially, as opposed
	// to calling them concurrently and merging the results. In the
	// common case, the primary backend is capable of pruning most
	// of the digests, making the call to the secondary backend a
	// lot smaller.
	missingInPrimary, err := ba.primary.FindMissing(ctx, digests)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Primary")
	}
	missingInBoth, err := ba.secondary.FindMissing(ctx, missingInPrimary)
	if err != nil {
		return digest.EmptySet, util.StatusWrap(err, "Secondary")
	}
	return missingInBoth, nil
}

type readFallbackErrorHandler struct {
	replicator replication.BlobReplicator
	context    context.Context
	digest     digest.Digest
}

func (eh *readFallbackErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	if status.Code(observedErr) != codes.NotFound {
		// One of the backends returned an error other than
		// NOT_FOUND. Prepend the name of the backend to make
		// debugging easier.
		if eh.replicator != nil {
			return nil, util.StatusWrap(observedErr, "Primary")
		}
		return nil, util.StatusWrap(observedErr, "Secondary")
	}
	if eh.replicator == nil {
		// We already tried the secondary below and got another
		// codes.NotFound, so just return that error.
		return nil, observedErr
	}

	// Run the replicator.
	r := eh.replicator
	eh.replicator = nil
	return r.ReplicateSingle(eh.context, eh.digest), nil
}

func (eh *readFallbackErrorHandler) Done() {}
