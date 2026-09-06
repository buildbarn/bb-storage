package cdc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingParametersFetcher struct {
	ParametersFetcher
	cache *ttlcache.TTLCache[digest.InstanceName, Parameters]
}

// NewCachingParametersFetcher decorates a ParametersFetcher with caching.
func NewCachingParametersFetcher(base ParametersFetcher, ttlCache *ttlcache.TTLCache[digest.InstanceName, Parameters]) ParametersFetcher {
	return &cachingParametersFetcher{
		ParametersFetcher: base,
		cache:             ttlCache,
	}
}

func (f *cachingParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (Parameters, error) {
	if v, ok := f.cache.Get(instanceName); ok {
		return v, nil
	}
	params, err := f.ParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return Parameters{}, err
	}
	f.cache.Put(instanceName, params)
	return params, nil
}
