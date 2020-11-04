package readcaching

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readCachingBlobAccess struct {
	slow       blobstore.BlobAccess
	fast       blobstore.BlobAccess
	replicator replication.BlobReplicator
}

// NewReadCachingBlobAccess turns a fast data store into a read cache
// for a slow data store. All writes are performed against the slow data
// store directly. The slow data store is only accessed for reading in
// case the fast data store does not contain the blob. The blob is then
// streamed into the fast data store using a replicator.
func NewReadCachingBlobAccess(slow, fast blobstore.BlobAccess, replicator replication.BlobReplicator) blobstore.BlobAccess {
	return &readCachingBlobAccess{
		slow:       slow,
		fast:       fast,
		replicator: replicator,
	}
}

func (ba *readCachingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.fast.Get(ctx, digest),
		&readCachingErrorHandler{
			replicator: ba.replicator,
			context:    ctx,
			digest:     digest,
		})
}

func (ba *readCachingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return ba.slow.Put(ctx, digest, b)
}

func (ba *readCachingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return ba.slow.FindMissing(ctx, digests)
}

type readCachingErrorHandler struct {
	replicator replication.BlobReplicator
	context    context.Context
	digest     digest.Digest
}

func (eh *readCachingErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	if eh.replicator == nil || status.Code(observedErr) != codes.NotFound {
		return nil, observedErr
	}
	replicator := eh.replicator
	eh.replicator = nil
	return replicator.ReplicateSingle(eh.context, eh.digest), nil
}

func (eh *readCachingErrorHandler) Done() {}
