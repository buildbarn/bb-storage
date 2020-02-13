package sharding

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"

	"go.opencensus.io/trace"
)

type shardingBlobAccess struct {
	backends           []blobstore.BlobAccess
	shardPermuter      ShardPermuter
	storageType        blobstore.StorageType
	hashInitialization uint64
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardPermuter is
// used to map hashes to backends.
func NewShardingBlobAccess(backends []blobstore.BlobAccess, shardPermuter ShardPermuter, storageType blobstore.StorageType, hashInitialization uint64) blobstore.BlobAccess {
	return &shardingBlobAccess{
		backends:           backends,
		shardPermuter:      shardPermuter,
		storageType:        storageType,
		hashInitialization: hashInitialization,
	}
}

func (ba *shardingBlobAccess) getBackend(digest *util.Digest) blobstore.BlobAccess {
	// Hash the key using FNV-1a.
	h := ba.hashInitialization
	for _, c := range ba.storageType.GetDigestKey(digest) {
		h ^= uint64(c)
		h *= 1099511628211
	}

	// Keep requesting shards until matching one that is undrained.
	var backend blobstore.BlobAccess
	ba.shardPermuter.GetShard(h, func(index int) bool {
		backend = ba.backends[index]
		return backend == nil
	})
	return backend
}

func (ba *shardingBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	ctx, span := trace.StartSpan(ctx, "blobstore.ShardingBlobAccess.Get")
	defer span.End()

	return ba.getBackend(digest).Get(ctx, digest)
}

func (ba *shardingBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	ctx, span := trace.StartSpan(ctx, "blobstore.ShardingBlobAccess.Put")
	defer span.End()

	return ba.getBackend(digest).Put(ctx, digest, b)
}

type findMissingResults struct {
	missing []*util.Digest
	err     error
}

func callFindMissing(ctx context.Context, blobAccess blobstore.BlobAccess, digests []*util.Digest) findMissingResults {
	missing, err := blobAccess.FindMissing(ctx, digests)
	return findMissingResults{missing: missing, err: err}
}

func (ba *shardingBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ctx, span := trace.StartSpan(ctx, "blobstore.ShardingBlobAccess.FindMissing")
	defer span.End()

	// Determine which backends to contact.
	digestsPerBackend := map[blobstore.BlobAccess][]*util.Digest{}
	for _, digest := range digests {
		backend := ba.getBackend(digest)
		digestsPerBackend[backend] = append(digestsPerBackend[backend], digest)
	}

	// Asynchronously call FindMissing() on backends.
	resultsChan := make(chan findMissingResults, len(digestsPerBackend))
	for backend, digests := range digestsPerBackend {
		go func(backend blobstore.BlobAccess, digests []*util.Digest) {
			resultsChan <- callFindMissing(ctx, backend, digests)
		}(backend, digests)
	}

	// Recombine results.
	var missingDigests []*util.Digest
	var err error
	for i := 0; i < len(digestsPerBackend); i++ {
		results := <-resultsChan
		if results.err == nil {
			missingDigests = append(missingDigests, results.missing...)
		} else {
			err = results.err
		}
	}
	return missingDigests, err
}
