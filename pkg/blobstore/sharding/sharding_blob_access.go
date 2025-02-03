package sharding

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
	backends             map[string]blobstore.BlobAccess
	shardSelector        ShardSelector
	hashInitialization   uint64
	getCapabilitiesRound atomic.Uint64
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardSelector is
// used to map hashes to backends.
func NewShardingBlobAccess(backends map[string]blobstore.BlobAccess, shardSelector ShardSelector, hashInitialization uint64) blobstore.BlobAccess {
	return &shardingBlobAccess{
		backends:           backends,
		shardSelector:      shardSelector,
		hashInitialization: hashInitialization,
	}
}

func (ba *shardingBlobAccess) getBackendKeyByDigest(blobDigest digest.Digest) string {
	// Hash the key using FNV-1a.
	h := ba.hashInitialization
	for _, c := range blobDigest.GetKey(digest.KeyWithoutInstance) {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return ba.getBackendKeyByHash(h)
}

func (ba *shardingBlobAccess) getBackendKeyByHash(h uint64) string {
	var selectedKey string = ba.shardSelector.GetShard(h)
	return selectedKey
}

func (ba *shardingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	key := ba.getBackendKeyByDigest(digest)
	return buffer.WithErrorHandler(
		ba.backends[key].Get(ctx, digest),
		shardKeyAddingErrorHandler{key: key})
}

func (ba *shardingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	key := ba.getBackendKeyByDigest(parentDigest)
	return buffer.WithErrorHandler(
		ba.backends[key].GetFromComposite(ctx, parentDigest, childDigest, slicer),
		shardKeyAddingErrorHandler{key: key})
}

func (ba *shardingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	key := ba.getBackendKeyByDigest(digest)
	if err := ba.backends[key].Put(ctx, digest, b); err != nil {
		return util.StatusWrapf(err, "Shard %s", key)
	}
	return nil
}

func (ba *shardingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition all digests by shard.
	digestsPerBackend := make(map[string]digest.SetBuilder, len(ba.backends))
	for key, _ := range ba.backends {
		digestsPerBackend[key] = digest.NewSetBuilder()
	}
	for _, blobDigest := range digests.Items() {
		digestsPerBackend[ba.getBackendKeyByDigest(blobDigest)].Add(blobDigest)
	}

	// Asynchronously call FindMissing() on backends.
	missingPerBackend := make([]digest.Set, 0, len(ba.backends))
	group, ctxWithCancel := errgroup.WithContext(ctx)
	for keyIter, digestsIter := range digestsPerBackend {
		key, digests := keyIter, digestsIter
		if digests.Length() > 0 {
			missingPerBackend = append(missingPerBackend, digest.EmptySet)
			missingOut := &missingPerBackend[len(missingPerBackend)-1]
			group.Go(func() error {
				missing, err := ba.backends[key].FindMissing(ctxWithCancel, digests.Build())
				if err != nil {
					return util.StatusWrapf(err, "Shard %s", key)
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
	key := ba.getBackendKeyByHash(ba.getCapabilitiesRound.Add(1))
	capabilities, err := ba.backends[key].GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %s", key)
	}
	return capabilities, nil
}

type shardKeyAddingErrorHandler struct {
	key string
}

func (eh shardKeyAddingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrapf(err, "Shard %s", eh.key)
}

func (eh shardKeyAddingErrorHandler) Done() {}
