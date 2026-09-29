package readcaching

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readCachingBlobAccess[T any] struct {
	blobstore.BlobAccess[T]
	fast       blobstore.BlobAccess[T]
	replicator replication.BlobReplicator
}

// NewReadCachingBlobAccess turns a fast data store into a read cache
// for a slow data store. All writes are performed against the slow data
// store directly. The slow data store is only accessed for reading in
// case the fast data store does not contain the blob. The blob is then
// streamed into the fast data store using a replicator.
func NewReadCachingBlobAccess[T any](slow, fast blobstore.BlobAccess[T], replicator replication.BlobReplicator) blobstore.BlobAccess[T] {
	return &readCachingBlobAccess[T]{
		BlobAccess: slow,
		fast:       fast,
		replicator: replicator,
	}
}

func (ba *readCachingBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	var zero T
	ret, err := ba.fast.Get(ctx, digest)
	if err == nil {
		return ret, nil
	}
	if status.Code(err) != codes.NotFound {
		return zero, util.StatusWrap(err, "Fast")
	}
	err = ba.replicator.ReplicateMultiple(ctx, digest.ToSingletonSet())
	if err != nil && status.Code(err) != codes.NotFound {
		return zero, err
	}
	return ba.fast.Get(ctx, digest)
}
