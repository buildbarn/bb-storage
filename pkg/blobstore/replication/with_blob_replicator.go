package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
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
		})
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

// GetFromCompositeWithBlobReplicator is equivalent to
// GetWithBlobReplicator, except that it's a common implementation of
// BlobAccess.GetFromComposite().
func GetFromCompositeWithBlobReplicator(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer, initialBackend blobstore.BlobAccess, successiveBackends BlobReplicatorSelector) buffer.Buffer {
	return buffer.WithErrorHandler(
		initialBackend.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		&getFromCompositeReplicatingErrorHandler{
			selector:     successiveBackends,
			context:      ctx,
			parentDigest: parentDigest,
			childDigest:  childDigest,
			slicer:       slicer,
		})
}

type getFromCompositeReplicatingErrorHandler struct {
	selector     BlobReplicatorSelector
	context      context.Context
	parentDigest digest.Digest
	childDigest  digest.Digest
	slicer       slicing.BlobSlicer
}

func (eh *getFromCompositeReplicatingErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	replicator, err := eh.selector(observedErr)
	if err != nil {
		return nil, err
	}
	return replicator.ReplicateComposite(eh.context, eh.parentDigest, eh.childDigest, eh.slicer), nil
}

func (getFromCompositeReplicatingErrorHandler) Done() {}
