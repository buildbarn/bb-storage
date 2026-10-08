package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type localBlobReplicator[T any] struct {
	source blobstore.BlobAccess[T]
	sink   blobstore.BlobAccess[T]
}

// NewLocalBlobReplicator creates a BlobReplicator that can be used to
// let MirroredBlobAccess repair inconsistencies between backends
// directly.
//
// This replicator tends to be sufficient for the Action Cache (AC), but
// for the Content Addressable Storage (CAS) it may be inefficient. If
// MirroredBlobAccess is used by many clients, each having a high
// concurrency, this replicator may cause redundant replications and
// load spikes. A separate replication daemon (bb_replicator) should be
// used for such setups.
func NewLocalBlobReplicator[T any](source, sink blobstore.BlobAccess[T]) BlobReplicator[T] {
	return &localBlobReplicator[T]{
		source: source,
		sink:   sink,
	}
}

func (br *localBlobReplicator[T]) ReplicateSingle(ctx context.Context, d digest.Digest) (T, error) {
	value, err := br.source.Get(ctx, d)
	if err != nil {
		var zero T
		return zero, err
	}
	if err := br.sink.Put(ctx, d, value); err != nil {
		return value, util.StatusWrap(err, d.String())
	}
	return value, nil
}

func (br *localBlobReplicator[T]) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	for _, blobDigest := range digests.Items() {
		val, err := br.source.Get(ctx, blobDigest)
		if err != nil {
			return util.StatusWrap(err, blobDigest.String())
		}
		if err := br.sink.Put(ctx, blobDigest, val); err != nil {
			return util.StatusWrap(err, blobDigest.String())
		}
	}
	return nil
}
