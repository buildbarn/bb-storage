package replication

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type replicatingBlob struct {
	finished <-chan struct{}
	success  bool
}

type deduplicatingBlobReplicator struct {
	base                BlobReplicator
	sink                blobstore.BlobAccess
	sinkDigestKeyFormat digest.KeyFormat

	lock                 sync.Mutex
	inFlightReplications map[string]*replicatingBlob
}

// NewDeduplicatingBlobReplicator creates a decorator for BlobReplicator
// that ensures that blobs are not replicated redundantly. Replication
// requests for the same blob are merged. To deal with potential race
// conditions, this replicator double checks whether the sink already
// contains a blob before copying.
//
// In order to guarantee responsiveness for all callers, this replicator
// decomposes requests for multiple blobs into one request per blob. To
// prevent callers from stalling the replication process, it also
// doesn't stream data back to the caller as it is being replicated.
// This means that blobs are fully replicated from the source to the
// sink, prior to letting the caller read the data from the sink at its
// own pace.
//
// This replicator has been designed to reduce the amount of traffic
// against the source to an absolute minimum, at the cost of generating
// more traffic against the sink. It is recommended to use this
// replicator when the sink is an instance of LocalBlobAccess that is
// embedded into the same process, and blobs are expected to be consumed
// locally.
func NewDeduplicatingBlobReplicator(base BlobReplicator, sink blobstore.BlobAccess, sinkDigestKeyFormat digest.KeyFormat) BlobReplicator {
	return &deduplicatingBlobReplicator{
		base:                 base,
		sink:                 sink,
		sinkDigestKeyFormat:  sinkDigestKeyFormat,
		inFlightReplications: map[string]*replicatingBlob{},
	}
}

func (br *deduplicatingBlobReplicator) ReplicateSingle(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	if err := br.ReplicateMultiple(ctx, blobDigest.ToSingletonSet()); err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.WithErrorHandler(
		br.sink.Get(ctx, blobDigest),
		notFoundToInternalErrorHandler{})
}

func (br *deduplicatingBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
NextDigest:
	for _, digest := range digests.Items() {
		// Register that we're about to replicate the blob.
		// Install a channel that allows others to wait for
		// replication to complete.
		key := digest.GetKey(br.sinkDigestKeyFormat)
		br.lock.Lock()
		for {
			replicatingBlob, ok := br.inFlightReplications[key]
			if !ok {
				// Nobody else is replicating this blob
				// right now, meaning that it's our turn.
				break
			}
			br.lock.Unlock()

			// Another caller is replicating this blob right
			// now. Wait for that to complete. Based on
			// whether that failed, retry or continue to the
			// next blob.
			select {
			case <-replicatingBlob.finished:
				if replicatingBlob.success {
					continue NextDigest
				}
				br.lock.Lock()
			case <-ctx.Done():
				return util.StatusFromContext(ctx)
			}
		}
		finished := make(chan struct{})
		replicatingBlob := replicatingBlob{finished: finished}
		br.inFlightReplications[key] = &replicatingBlob
		br.lock.Unlock()

		// Now that we're exclusively responsible for
		// synchronizing this blob, replicate it. Do check up
		// front whether it already exists, as another caller
		// might have done that already.
		singleDigest := digest.ToSingletonSet()
		missing, err := br.sink.FindMissing(ctx, singleDigest)
		if err != nil {
			err = util.StatusWrapf(err, "Failed to check for the existence of blob %s prior to replicating", digest)
		} else if !missing.Empty() {
			err = br.base.ReplicateMultiple(ctx, singleDigest)
			if err != nil {
				err = util.StatusWrapf(err, "Failed to replicate blob %s", digest)
			}
		}

		// Wake up other callers.
		br.lock.Lock()
		delete(br.inFlightReplications, key)
		br.lock.Unlock()
		replicatingBlob.success = err == nil
		close(finished)
		if err != nil {
			return err
		}
	}
	return nil
}
