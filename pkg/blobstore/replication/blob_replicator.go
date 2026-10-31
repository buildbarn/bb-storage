package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlobReplicator is an interface that provides the strategy that
// replicates blobs between a sink and a source blob access. It is used
// by e.g. MirroredBlobAccess to replicate objects it detects that a
// certain object is only present in only one of the two backends.
type BlobReplicator[T any] interface {
	// ReplicateSingle replicates a single object between backends,
	// returning the object.
	ReplicateSingle(ctx context.Context, d digest.Digest) (T, error)

	// ReplicateMultiple replicates a set of objects between backends.
	ReplicateMultiple(ctx context.Context, digests digest.Set) error
}
