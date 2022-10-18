package readcaching

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readCachingBlobAccess struct {
	blobstore.BlobAccess
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
		BlobAccess: slow,
		fast:       fast,
		replicator: replicator,
	}
}

func (ba *readCachingBlobAccess) getBlobReplicatorSelector() replication.BlobReplicatorSelector {
	replicator := ba.replicator
	return func(observedErr error) (replication.BlobReplicator, error) {
		if replicator == nil || status.Code(observedErr) != codes.NotFound {
			return nil, observedErr
		}
		replicatorToReturn := replicator
		replicator = nil
		return replicatorToReturn, nil
	}
}

func (ba *readCachingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return replication.GetWithBlobReplicator(
		ctx,
		digest,
		ba.fast,
		ba.getBlobReplicatorSelector())
}

func (ba *readCachingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return replication.GetFromCompositeWithBlobReplicator(
		ctx,
		parentDigest,
		childDigest,
		slicer,
		ba.fast,
		ba.getBlobReplicatorSelector())
}
