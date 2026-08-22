package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Replicator replicates blobs from a source Content Addressable Storage
// (CAS) to a sink CAS.
type Replicator interface {
	// Replicate blobs from the set of digests.
	Replicate(ctx context.Context, digests digest.Set) error
}
