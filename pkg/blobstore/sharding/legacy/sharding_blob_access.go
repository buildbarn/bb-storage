package legacy

import (
	"context"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
)

type shardingBlobAccess struct {
	backends             []blobstore.BlobAccess
	shardPermuter        ShardPermuter
	hashInitialization   uint64
	getCapabilitiesRound atomic.Uint64
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions requests
// across backends by hashing the digest. A ShardPermuter is used to map hashes
// to backends.
func NewShardingBlobAccess(backends []blobstore.BlobAccess, shardPermuter ShardPermuter, hashInitialization uint64) blobstore.BlobAccess {
	return &shardingBlobAccess{
		backends:           backends,
		shardPermuter:      shardPermuter,
		hashInitialization: hashInitialization,
	}
}

func (ba *shardingBlobAccess) getBackendIndexByDigest(blobDigest digest.Digest) int {
	// Hash the key using FNV-1a.
	h := ba.hashInitialization
	for _, c := range blobDigest.GetKey(digest.KeyWithoutInstance) {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return ba.getBackendIndexByHash(h)
}

func (ba *shardingBlobAccess) getBackendIndexByHash(h uint64) int {
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
	index := ba.getBackendIndexByDigest(digest)
	return buffer.WithErrorHandler(
		ba.backends[index].Get(ctx, digest),
		shardIndexAddingErrorHandler{index: index})
}

func (ba *shardingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	index := ba.getBackendIndexByDigest(parentDigest)
	return buffer.WithErrorHandler(
		ba.backends[index].GetFromComposite(ctx, parentDigest, childDigest, slicer),
		shardIndexAddingErrorHandler{index: index})
}

func (ba *shardingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	index := ba.getBackendIndexByDigest(digest)
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
		digestsPerBackend[ba.getBackendIndexByDigest(blobDigest)].Add(blobDigest)
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

func (ba *shardingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Spread requests across shards.
	index := ba.getBackendIndexByHash(ba.getCapabilitiesRound.Add(1))
	capabilities, err := ba.backends[index].GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %d", index)
	}
	return capabilities, nil
}

type shardIndexAddingErrorHandler struct {
	index int
}

func (eh shardIndexAddingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrapf(err, "Shard %d", eh.index)
}

func (eh shardIndexAddingErrorHandler) Done() {}
