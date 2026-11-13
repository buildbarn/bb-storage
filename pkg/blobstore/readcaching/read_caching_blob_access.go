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
	replicator replication.BlobReplicator[T]
}

// NewReadCachingBlobAccess turns a fast data store into a read cache
// for a slow data store. All writes are performed against the slow data
// store directly. The slow data store is only accessed for reading in
// case the fast data store does not contain the blob. The blob is then
// streamed into the fast data store using a replicator.
func NewReadCachingBlobAccess[T any](slow, fast blobstore.BlobAccess[T], replicator replication.BlobReplicator[T]) blobstore.BlobAccess[T] {
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
	ret, err = ba.replicator.ReplicateSingle(ctx, digest)
	if err != nil && status.Code(err) != codes.NotFound {
		return zero, err
	}
	return ret, err
}

// Prefetch ensures that a set of objects is present in the fast data
// store, replicating them from the slow data store where needed. This
// allows callers to warm up the read cache without having to perform a
// Get() for every object.
//
// Objects that are absent from the slow data store cause a NOT_FOUND
// error to be returned.
func (ba *readCachingBlobAccess[T]) Prefetch(ctx context.Context, digests digest.Set) error {
	// Determine which objects are still missing from the fast data
	// store, so that objects that are already cached are not fetched
	// from the slow data store again.
	missing, err := ba.fast.FindMissing(ctx, digests)
	if err != nil {
		return util.StatusWrap(err, "Fast")
	}
	if missing.Empty() {
		return nil
	}
	return ba.replicator.ReplicateMultiple(ctx, missing)
}
