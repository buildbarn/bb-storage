package sharding

import (
	"context"
	"encoding/binary"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
)

type shardingBlobAccess[T any] struct {
	backends             []ShardBackend[T]
	shardSelector        ShardSelector
	getCapabilitiesRound atomic.Uint64
}

// ShardBackend is the Backend together with its key, the key is used for error
// messages.
type ShardBackend[T any] struct {
	Backend blobstore.BlobAccess[T]
	Key     string
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardSelector is
// used to map hashes to backends.
func NewShardingBlobAccess[T any](backends []ShardBackend[T], shardSelector ShardSelector) blobstore.BlobAccess[T] {
	return &shardingBlobAccess[T]{
		backends:      backends,
		shardSelector: shardSelector,
	}
}

func (ba *shardingBlobAccess[T]) getBackendIndexByDigest(blobDigest digest.Digest) int {
	// Use the first 8 bytes of the digest hash for calculating backend.
	hb := blobDigest.GetHashBytes()
	h := binary.BigEndian.Uint64(hb[:8])
	return ba.shardSelector.GetShard(h)
}

func (ba *shardingBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	index := ba.getBackendIndexByDigest(digest)
	var zero T
	ret, err := ba.backends[index].Backend.Get(ctx, digest)
	if err != nil {
		return zero, util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return ret, nil
}

func (ba *shardingBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	index := ba.getBackendIndexByDigest(digest)
	if err := ba.backends[index].Backend.Put(ctx, digest, value); err != nil {
		return util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return nil
}

func (ba *shardingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition all digests by shard.
	digestsPerBackend := make([]digest.SetBuilder, 0, len(ba.backends))
	for range ba.backends {
		digestsPerBackend = append(digestsPerBackend, digest.NewSetBuilder(digests.Length()/len(ba.backends)+1))
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

func (ba *shardingBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Spread requests across shards.
	index := ba.shardSelector.GetShard(ba.getCapabilitiesRound.Add(1))
	capabilities, err := ba.backends[index].Backend.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return capabilities, nil
}
