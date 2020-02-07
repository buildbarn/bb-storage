package mirrored

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlobReplicator provides the strategy that is used by
// MirroredBlobAccess to replicate objects between storage backends.
// This strategy is called into when MirroredBlobAccess detects that a
// certain object is only present in one of the two backends.
type BlobReplicator interface {
	// Replicate a single object between backends, while at the same
	// time giving a handle back to it.
	ReplicateSingle(ctx context.Context, digest digest.Digest) buffer.Buffer
	// Replicate a set of objects between backends.
	ReplicateMultiple(ctx context.Context, digests digest.Set) error
}
