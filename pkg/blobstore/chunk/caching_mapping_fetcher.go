package chunk

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingMappingFetcher struct {
	MappingFetcher
	cache *ttlcache.TTLCache[digest.Digest, Mapping]
}

// NewCachingMappingFetcher decorates a Fetcher with a cache.
func NewCachingMappingFetcher(base MappingFetcher, cache *ttlcache.TTLCache[digest.Digest, Mapping]) MappingFetcher {
	return &cachingMappingFetcher{
		MappingFetcher: base,
		cache:          cache,
	}
}

func (f *cachingMappingFetcher) FetchChunkMapping(ctx context.Context, d digest.Digest) (Mapping, error) {
	if v, ok := f.cache.Get(d); ok {
		return v, nil
	}
	chunkMapping, err := f.MappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return Mapping{}, err
	}
	f.cache.Put(d, chunkMapping)
	return chunkMapping, nil
}
