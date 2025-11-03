package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type noopBlobReplicator struct {
	source blobstore.BlobAccess
}

// NewNoopBlobReplicator creates a BlobReplicator that can be used to
// access a single source without replication.
//
// It is useful for the BlobAccess variants where replication is optional.
func NewNoopBlobReplicator(source blobstore.BlobAccess) BlobReplicator {
	return noopBlobReplicator{
		source: source,
	}
}

func (br noopBlobReplicator) ReplicateSingle(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return br.source.Get(ctx, digest)
}

func (br noopBlobReplicator) ReplicateComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return br.source.GetFromComposite(ctx, parentDigest, childDigest, slicer)
}

func (noopBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	return nil
}
