package chunk

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/lossymap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingMappingFetcher struct {
	MappingFetcher
	clock clock.Clock
	cache lossymap.Map[digest.Digest, Mapping, time.Time]
	lock  sync.Mutex
}

// NewCachingMappingFetcher decorates a Fetcher with a cache.
func NewCachingMappingFetcher(base MappingFetcher, clock clock.Clock, cache lossymap.Map[digest.Digest, Mapping, time.Time]) MappingFetcher {
	return &cachingMappingFetcher{
		MappingFetcher: base,
		clock:          clock,
		cache:          cache,
	}
}

func (f *cachingMappingFetcher) FetchChunkMapping(ctx context.Context, d digest.Digest) (Mapping, error) {
	f.lock.Lock()
	chunkMapping, err := f.cache.Get(d, f.clock.Now())
	f.lock.Unlock()
	if err == nil {
		return chunkMapping, nil
	} else if status.Code(err) != codes.NotFound {
		return Mapping{}, err
	}
	chunkMapping, err = f.MappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return Mapping{}, err
	}
	f.lock.Lock()
	err = f.cache.Put(d, chunkMapping, f.clock.Now())
	f.lock.Unlock()
	if err != nil {
		return Mapping{}, err
	}
	return chunkMapping, nil
}
