package mirrored

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
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
func NewLocalBlobReplicator(source blobstore.BlobAccess, sink blobstore.BlobAccess) BlobReplicator {
	return &localBlobReplicator{
		source: source,
		sink:   sink,
	}
}

func (br *localBlobReplicator) ReplicateSingle(ctx context.Context, digest digest.Digest) buffer.Buffer {
	b1, b2 := br.source.Get(ctx, digest).CloneStream()
	b1, t := buffer.WithBackgroundTask(b1)
	go func() {
		err := br.sink.Put(ctx, digest, b2)
		if err != nil {
			err = util.StatusWrap(err, "Replication failed")
		}
		t.Finish(err)
	}()
	return b1
}

func (br *localBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	for _, blobDigest := range digests.Items() {
		if err := br.sink.Put(ctx, blobDigest, br.source.Get(ctx, blobDigest)); err != nil {
			return util.StatusWrap(err, blobDigest.String())
		}
	}
	return nil
}
