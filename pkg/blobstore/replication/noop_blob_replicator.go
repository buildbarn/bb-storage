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
func NewNoopBlobReplicator[T any](source blobstore.BlobAccess[T]) BlobReplicator {
	return noopBlobReplicator[T]{
		source: source,
	}
}

func (noopBlobReplicator[T]) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	return nil
}
