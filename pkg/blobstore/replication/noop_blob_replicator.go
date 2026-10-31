package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type noopBlobReplicator[T any] struct {
	source blobstore.BlobAccess[T]
}

// NewNoopBlobReplicator creates a BlobReplicator that can be used to
// access a single source without replication.
//
// It is useful for the BlobAccess variants where replication is optional.
func NewNoopBlobReplicator[T any](source blobstore.BlobAccess[T]) BlobReplicator[T] {
	return noopBlobReplicator[T]{
		source: source,
	}
}

func (br noopBlobReplicator[T]) ReplicateSingle(ctx context.Context, d digest.Digest) (T, error) {
	return br.source.Get(ctx, d)
}

func (noopBlobReplicator[T]) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	return nil
}
