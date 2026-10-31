package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/semaphore"
)

type concurrencyLimitingBlobReplicator[T any] struct {
	base      BlobReplicator[T]
	sink      blobstore.BlobAccess[T]
	semaphore *semaphore.Weighted
}

// NewConcurrencyLimitingBlobReplicator creates a decorator for
// BlobReplicator that uses a semaphore to place a limit on the number
// of concurrent replication requests. This can be used to prevent
// excessive amounts of congestion on the network.
//
// The semaphore.Weighted type retains the original request order,
// meaning that starvation is prevented.
func NewConcurrencyLimitingBlobReplicator[T any](base BlobReplicator[T], sink blobstore.BlobAccess[T], semaphore *semaphore.Weighted) BlobReplicator[T] {
	return &concurrencyLimitingBlobReplicator[T]{
		base:      base,
		sink:      sink,
		semaphore: semaphore,
	}
}

func (br *concurrencyLimitingBlobReplicator[T]) ReplicateSingle(ctx context.Context, d digest.Digest) (T, error) {
	// Replicate the object from the source to the sink before
	// returning a copy to the caller. Because this replicator
	// performs queueing, we can't allow the caller to influence the
	// speed at which the object is replicated.
	if err := br.ReplicateMultiple(ctx, d.ToSingletonSet()); err != nil {
		var zero T
		return zero, err
	}
	return br.sink.Get(ctx, d)
}

func (br *concurrencyLimitingBlobReplicator[T]) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	if err := util.AcquireSemaphore(ctx, br.semaphore, 1); err != nil {
		return err
	}
	err := br.base.ReplicateMultiple(ctx, digests)
	br.semaphore.Release(1)
	return err
}
