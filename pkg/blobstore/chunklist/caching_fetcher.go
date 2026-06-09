package chunklist

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingFetcher struct {
	Fetcher
	cache *ttlcache.TTLCache[digest.Digest, ChunkList]
}

// NewCachingFetcher decorates a Fetcher with a cache.
func NewCachingFetcher(base Fetcher, cache *ttlcache.TTLCache[digest.Digest, ChunkList]) Fetcher {
	return &cachingFetcher{
		Fetcher: base,
		cache:   cache,
	}
}

func (f *cachingFetcher) FetchChunkList(ctx context.Context, d digest.Digest) (ChunkList, error) {
	if v, ok := f.cache.Get(d); ok {
		return v, nil
	}
	chunkList, err := f.Fetcher.FetchChunkList(ctx, d)
	if err != nil {
		return nil, err
	}
	f.cache.Put(d, chunkList)
	return chunkList, nil
}
