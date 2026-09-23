package chunk

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingListFetcher struct {
	ListFetcher
	cache *ttlcache.TTLCache[digest.Digest, List]
}

// NewCachingListFetcher decorates a Fetcher with a cache.
func NewCachingListFetcher(base ListFetcher, cache *ttlcache.TTLCache[digest.Digest, List]) ListFetcher {
	return &cachingListFetcher{
		ListFetcher: base,
		cache:       cache,
	}
}

func (f *cachingListFetcher) FetchChunkList(ctx context.Context, d digest.Digest) (List, error) {
	if v, ok := f.cache.Get(d); ok {
		return v, nil
	}
	chunkList, err := f.ListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return List{}, err
	}
	f.cache.Put(d, chunkList)
	return chunkList, nil
}
