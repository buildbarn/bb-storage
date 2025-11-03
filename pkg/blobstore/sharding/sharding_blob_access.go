package sharding

import (
	"context"
	"encoding/binary"
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
	backends             []ShardBackend
	shardSelector        ShardSelector
	getCapabilitiesRound atomic.Uint64
}

// ShardBackend is the Backend together with its key, the key is used for error
// messages.
type ShardBackend struct {
	Backend blobstore.BlobAccess
	Key     string
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardSelector is
// used to map hashes to backends.
func NewShardingBlobAccess(backends []ShardBackend, shardSelector ShardSelector) blobstore.BlobAccess {
	return &shardingBlobAccess{
		backends:      backends,
		shardSelector: shardSelector,
	}
}

func (ba *shardingBlobAccess) getBackendIndexByDigest(blobDigest digest.Digest) int {
	// Use the first 8 bytes of the digest hash for calculating backend.
	hb := blobDigest.GetHashBytes()
	h := binary.BigEndian.Uint64(hb[:8])
	return ba.shardSelector.GetShard(h)
}

func (ba *shardingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	index := ba.getBackendIndexByDigest(digest)
	return buffer.WithErrorHandler(
		ba.backends[index].Backend.Get(ctx, digest),
		shardKeyAddingErrorHandler{key: ba.backends[index].Key})
}

func (ba *shardingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	index := ba.getBackendIndexByDigest(parentDigest)
	return buffer.WithErrorHandler(
		ba.backends[index].Backend.GetFromComposite(ctx, parentDigest, childDigest, slicer),
		shardKeyAddingErrorHandler{key: ba.backends[index].Key})
}

func (ba *shardingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	index := ba.getBackendIndexByDigest(digest)
	if err := ba.backends[index].Backend.Put(ctx, digest, b); err != nil {
		return util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
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
				missing, err := ba.backends[index].Backend.FindMissing(ctxWithCancel, digests.Build())
				if err != nil {
					return util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
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
	index := ba.shardSelector.GetShard(ba.getCapabilitiesRound.Add(1))
	capabilities, err := ba.backends[index].Backend.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return capabilities, nil
}

type shardKeyAddingErrorHandler struct {
	key string
}

func (eh shardKeyAddingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, util.StatusWrapf(err, "Shard %s", eh.key)
}

func (shardKeyAddingErrorHandler) Done() {}
