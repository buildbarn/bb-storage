package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type queuedBlobReplicator struct {
	source         blobstore.BlobAccess
	base           BlobReplicator
	existenceCache *digest.ExistenceCache
	wait           chan struct{}
}

// NewQueuedBlobReplicator creates a decorator for BlobReplicator that
// serializes and deduplicates requests. It can be used to place a limit
// on the amount of replication traffic.
//
// TODO: The current implementation is a bit simplistic, in that it does
// not guarantee fairness. Should all requests be processed in FIFO
// order? Alternatively, should we replicate objects with most waiters
// first?
func NewQueuedBlobReplicator(source blobstore.BlobAccess, base BlobReplicator, existenceCache *digest.ExistenceCache) BlobReplicator {
	q := &queuedBlobReplicator{
		source:         source,
		base:           base,
		existenceCache: existenceCache,
		wait:           make(chan struct{}, 1),
	}
	q.wait <- struct{}{}
	return q
}

func (br *queuedBlobReplicator) ReplicateSingle(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	// Serve the read request from the source, while letting the
	// replication go through the regular queueing process.
	//
	// This causes a duplicate read on the source, but this cannot
	// be prevented reasonably. The client and the replication
	// process may each run at a different pace.
	b := br.source.Get(ctx, blobDigest)
	b, t := buffer.WithBackgroundTask(b)
	go func() {
		err := br.ReplicateMultiple(ctx, digest.NewSetBuilder().Add(blobDigest).Build())
		if err != nil {
			err = util.StatusWrap(err, "Replication failed")
		}
		t.Finish(err)
	}()
	return b
}

func (br *queuedBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	// Don't queue requests for objects that have already been
	// replicated.
	if br.existenceCache.RemoveExisting(digests).Empty() {
		return nil
	}

	// Queue the request.
	select {
	case <-br.wait:
	case <-ctx.Done():
		return util.StatusFromContext(ctx)
	}

	// Forward the call, filtering out objects that have already
	// been replicated.
	digests = br.existenceCache.RemoveExisting(digests)
	err := br.base.ReplicateMultiple(ctx, digests)
	if err == nil {
		br.existenceCache.Add(digests)
	}

	// Unblock the next request.
	br.wait <- struct{}{}
	return err
}
