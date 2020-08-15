package sharding

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type shardingBlobAccess struct {
	backends           []blobstore.BlobAccess
	shardPermuter      ShardPermuter
	digestKeyFormat    digest.KeyFormat
	hashInitialization uint64
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardPermuter is
// used to map hashes to backends.
func NewShardingBlobAccess(backends []blobstore.BlobAccess, shardPermuter ShardPermuter, digestKeyFormat digest.KeyFormat, hashInitialization uint64) blobstore.BlobAccess {
	return &shardingBlobAccess{
		backends:           backends,
		shardPermuter:      shardPermuter,
		digestKeyFormat:    digestKeyFormat,
		hashInitialization: hashInitialization,
	}
}

func (ba *shardingBlobAccess) getBackend(digest digest.Digest) blobstore.BlobAccess {
	// Hash the key using FNV-1a.
	h := ba.hashInitialization
	for _, c := range digest.GetKey(ba.digestKeyFormat) {
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

func (ba *shardingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return ba.getBackend(digest).Get(ctx, digest)
}

func (ba *shardingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	return ba.getBackend(digest).Put(ctx, digest, b)
}

type findMissingResults struct {
	missing digest.Set
	err     error
}

func callFindMissing(ctx context.Context, blobAccess blobstore.BlobAccess, digests digest.Set) findMissingResults {
	missing, err := blobAccess.FindMissing(ctx, digests)
	return findMissingResults{missing: missing, err: err}
}

func (ba *shardingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Determine which backends to contact.
	digestsPerBackend := map[blobstore.BlobAccess]digest.SetBuilder{}
	for _, blobDigest := range digests.Items() {
		backend := ba.getBackend(blobDigest)
		if _, ok := digestsPerBackend[backend]; !ok {
			digestsPerBackend[backend] = digest.NewSetBuilder()
		}
		digestsPerBackend[backend].Add(blobDigest)
	}

	// Asynchronously call FindMissing() on backends.
	resultsChan := make(chan findMissingResults, len(digestsPerBackend))
	for backend, digests := range digestsPerBackend {
		go func(backend blobstore.BlobAccess, digests digest.SetBuilder) {
			resultsChan <- callFindMissing(ctx, backend, digests.Build())
		}(backend, digests)
	}

	// Recombine results.
	missingDigestSets := make([]digest.Set, 0, len(digestsPerBackend))
	var err error
	for i := 0; i < len(digestsPerBackend); i++ {
		results := <-resultsChan
		if results.err == nil {
			missingDigestSets = append(missingDigestSets, results.missing)
		} else {
			err = results.err
		}
	}
	if err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion(missingDigestSets), nil
}
