package sharding

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
)

type shardingBlobAccess struct {
	backends           []blobstore.BlobAccess
	shardPermuter      ShardPermuter
	hashInitialization uint64
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardPermuter is
// used to map hashes to backends.
func NewShardingBlobAccess(backends []blobstore.BlobAccess, shardPermuter ShardPermuter, hashInitialization uint64) blobstore.BlobAccess {
	return &shardingBlobAccess{
		backends:           backends,
		shardPermuter:      shardPermuter,
		hashInitialization: hashInitialization,
	}
}

func (ba *shardingBlobAccess) getBackendIndex(blobDigest digest.Digest) int {
	// Hash the key using FNV-1a.
	h := ba.hashInitialization
	for _, c := range blobDigest.GetKey(digest.KeyWithoutInstance) {
		h ^= uint64(c)
		h *= 1099511628211
	}

	// Keep requesting shards until matching one that is undrained.
	var selectedIndex int
	ba.shardPermuter.GetShard(h, func(index int) bool {
		if ba.backends[index] == nil {
			return true
		}
		selectedIndex = index
		return false
	})
	return selectedIndex
}

func (ba *shardingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	index := ba.getBackendIndex(digest)
	return buffer.WithErrorHandler(
		ba.backends[index].Get(ctx, digest),
		shardIndexAddingErrorHandler{index: index})
}

func (ba *shardingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	index := ba.getBackendIndex(digest)
	if err := ba.backends[index].Put(ctx, digest, b); err != nil {
		return util.StatusWrapf(err, "Shard %d", index)
	}
	return nil
}

func (ba *shardingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition all digests by shard.
	digestsPerBackend := make([]digest.SetBuilder, 0, len(ba.backends))
	for range ba.backends {
		digestsPerBackend = append(digestsPerBackend, digest.NewSetBuilder())
	}
	for _, blobDigest := range digests.Items() {
		digestsPerBackend[ba.getBackendIndex(blobDigest)].Add(blobDigest)
	}

	// Asynchronously call FindMissing() on backends.
	missingPerBackend := make([]digest.Set, 0, len(ba.backends))
	group, ctxWithCancel := errgroup.WithContext(ctx)
	for indexIter, digestsIter := range digestsPerBackend {
		index, digests := indexIter, digestsIter
		if digests.Length() > 0 {
			missingPerBackend = append(missingPerBackend, digest.EmptySet)
			missingOut := &missingPerBackend[len(missingPerBackend)-1]
			group.Go(func() error {
				missing, err := ba.backends[index].FindMissing(ctxWithCancel, digests.Build())
				if err != nil {
					return util.StatusWrapf(err, "Shard %d", index)
				}
				*missingOut = missing
				return nil
			})
		}
	}

	// Recombine results.
	if err := group.Wait(); err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion(missingPerBackend), nil
}

type shardIndexAddingErrorHandler struct {
	index int
}

func (eh shardIndexAddingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrapf(err, "Shard %d", eh.index)
}

func (eh shardIndexAddingErrorHandler) Done() {}
