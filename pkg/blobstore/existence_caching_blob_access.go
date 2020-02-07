package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type existenceCachingBlobAccess struct {
	BlobAccess
	existenceCache *digest.ExistenceCache
}

// NewExistenceCachingBlobAccess creates a decorator for BlobAccess that
// adds caching to the FindMissing() operation.
//
// Clients such as Bazel tend to frequently call
// ContentAddressableStorage.FindMissingBlobs() with overlapping sets of
// digests. They don't seem to have a local cache of which digests they
// queried recently. This decorator adds such a cache.
//
// This decorator may be useful to run on instances that act as
// frontends for a mirrored/sharding storage pool, as it may reduce the
// load observed on the storage pool.
func NewExistenceCachingBlobAccess(base BlobAccess, existenceCache *digest.ExistenceCache) BlobAccess {
	return &existenceCachingBlobAccess{
		BlobAccess:     base,
		existenceCache: existenceCache,
	}
}

func (ba *existenceCachingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Determine which digests don't need to be checked, because
	// they have already been requested recently.
	maybeMissing := ba.existenceCache.RemoveExisting(digests)

	// Check existence of the remaining digests.
	missing, err := ba.BlobAccess.FindMissing(ctx, maybeMissing)
	if err != nil {
		return digest.EmptySet, err
	}

	// Insert the digests that were present for future calls.
	present, _, _ := digest.GetDifferenceAndIntersection(maybeMissing, missing)
	ba.existenceCache.Add(present)
	return missing, nil
}
