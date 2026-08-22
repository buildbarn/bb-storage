package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type blobAccessReplicator struct {
	replicator replication.BlobReplicator
}

// NewBlobAccessReplicator returns a cas.Replicator from a
// replication.BlobReplicator.
func NewBlobAccessReplicator(replicator replication.BlobReplicator) Replicator {
	return &blobAccessReplicator{
		replicator: replicator,
	}
}

func (r *blobAccessReplicator) Replicate(ctx context.Context, digests digest.Set) error {
	return r.replicator.ReplicateMultiple(ctx, digests)
}
