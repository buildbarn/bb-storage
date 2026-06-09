package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlobReplicatorSelector is called into by GetWithBlobReplicator to
// obtain a BlobReplicator that is used after a failure has been
// observed.
type BlobReplicatorSelector func(observedErr error) (BlobReplicator, error)

// GetWithBlobReplicator is a common implementation of BlobAccess.Get()
// that can be used by backends that call into a single backend, and
// fall back to calling into a BlobReplicator upon failure. This is a
// common pattern, used by backends such as MirroredBlobAccess and
// ReadCachingBlobAccess.
func GetWithBlobReplicator(ctx context.Context, digest digest.Digest, initialBackend blobstore.BlobAccess, successiveBackends BlobReplicatorSelector) buffer.Buffer {
	return buffer.WithErrorHandler(
		initialBackend.Get(ctx, digest),
		&getReplicatingErrorHandler{
			selector: successiveBackends,
			context:  ctx,
			digest:   digest,
		},
	)
}

type getReplicatingErrorHandler struct {
	selector BlobReplicatorSelector
	context  context.Context
	digest   digest.Digest
}

func (eh *getReplicatingErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	replicator, err := eh.selector(observedErr)
	if err != nil {
		return nil, err
	}
	return replicator.ReplicateSingle(eh.context, eh.digest), nil
}

func (getReplicatingErrorHandler) Done() {}
