package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readFallbackBlobAccess struct {
	primary   BlobAccess
	secondary BlobAccess
}

// NewReadFallbackBlobAccess creates a decorator for BlobAccess that
// causes reads for non-existent to be forwarded to a secondary storage
// backend. Data is never written to the latter.
//
// This decorator can be used to integrate external data sets into the
// system, e.g. by combining it with ReferenceExpandingBlobAccess.
func NewReadFallbackBlobAccess(primary BlobAccess, secondary BlobAccess) BlobAccess {
	return &readFallbackBlobAccess{
		primary:   primary,
		secondary: secondary,
	}
}

func (ba *readFallbackBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.primary.Get(ctx, digest),
		&readFallbackErrorHandler{
			secondary: ba.secondary,
			context:   ctx,
			digest:    digest,
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
	secondary BlobAccess
	context   context.Context
	digest    digest.Digest
}

func (eh *readFallbackErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	if status.Code(observedErr) != codes.NotFound {
		// One of the backends returned an error other than
		// NOT_FOUND. Prepend the name of the backend to make
		// debugging easier.
		if eh.secondary != nil {
			return nil, util.StatusWrap(observedErr, "Primary")
		}
		return nil, util.StatusWrap(observedErr, "Secondary")
	}
	if eh.secondary == nil {
		// Both backends returned NOT_FOUND. Don't bother
		// prepending the name of the backend.
		return nil, observedErr
	}
	ba := eh.secondary
	eh.secondary = nil
	return ba.Get(eh.context, eh.digest), nil
}

func (eh *readFallbackErrorHandler) Done() {}
