package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type localBlobReplicator struct {
	source blobstore.BlobAccess
	sink   blobstore.BlobAccess
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
func NewLocalBlobReplicator(source, sink blobstore.BlobAccess) BlobReplicator {
	return &localBlobReplicator{
		source: source,
		sink:   sink,
	}
}

func (br *localBlobReplicator) ReplicateSingle(ctx context.Context, digest digest.Digest) buffer.Buffer {
	b1, b2 := br.source.Get(ctx, digest).CloneStream()
	return b1.WithTask(func() error {
		if err := br.sink.Put(ctx, digest, b2); err != nil {
			return util.StatusWrap(err, "Replication failed")
		}
		return nil
	})
}

func (br *localBlobReplicator) ReplicateComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	// First replicate the object to the sink, and then read it back
	// again. Even though cloning like in ReplicateSingle() is more
	// optimal in terms of bandwidth, it would require us to do the
	// slicing here. This may cause slicing information to get lost.
	if err := br.ReplicateMultiple(ctx, parentDigest.ToSingletonSet()); err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.WithErrorHandler(
		br.sink.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		notFoundToInternalErrorHandler{})
}

func (br *localBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	for _, blobDigest := range digests.Items() {
		if err := br.sink.Put(ctx, blobDigest, br.source.Get(ctx, blobDigest)); err != nil {
			return util.StatusWrap(err, blobDigest.String())
		}
	}
	return nil
}
