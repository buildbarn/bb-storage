package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
)

type concurrencyLimitingBlobReplicator struct {
	base      BlobReplicator
	sink      blobstore.BlobAccess
	semaphore *semaphore.Weighted
}

// NewConcurrencyLimitingBlobReplicator creates a decorator for
// BlobReplicator that uses a semaphore to place a limit on the number
// of concurrent replication requests. This can be used to prevent
// excessive amounts of congestion on the network.
//
// The semaphore.Weighted type retains the original request order,
// meaning that starvation is prevented.
func NewConcurrencyLimitingBlobReplicator(base BlobReplicator, sink blobstore.BlobAccess, semaphore *semaphore.Weighted) BlobReplicator {
	return &concurrencyLimitingBlobReplicator{
		base:      base,
		sink:      sink,
		semaphore: semaphore,
	}
}

func (br *concurrencyLimitingBlobReplicator) ReplicateSingle(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	// Replicate the object from the source to the sink before
	// returning a copy to the caller. Because this replicator
	// performs queueing, we can't allow the caller to influence the
	// speed at which the object is replicated.
	if err := br.ReplicateMultiple(ctx, blobDigest.ToSingletonSet()); err != nil {
		return buffer.NewBufferFromError(err)
	}
	return br.sink.Get(ctx, blobDigest)
}

func (br *concurrencyLimitingBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	if br.semaphore.Acquire(ctx, 1) != nil {
		return util.StatusFromContext(ctx)
	}
	err := br.base.ReplicateMultiple(ctx, digests)
	br.semaphore.Release(1)
	return err
}
