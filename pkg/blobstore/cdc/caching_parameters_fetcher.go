package cdc

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingParametersFetcher struct {
	ParametersFetcher
	cache *ttlcache.TTLCache[digest.InstanceName, *remoteexecution.RepMaxCdcParams]
}

// NewCachingParametersFetcher decorates a ParametersFetcher with caching.
func NewCachingParametersFetcher(base ParametersFetcher, ttlCache *ttlcache.TTLCache[digest.InstanceName, *remoteexecution.RepMaxCdcParams]) ParametersFetcher {
	return &cachingParametersFetcher{
		ParametersFetcher: base,
		cache:             ttlCache,
	}
}

func (f *cachingParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	if params, ok := f.cache.Get(instanceName); ok {
		return params, nil
	}
	params, err := f.ParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	f.cache.Put(instanceName, params)
	return params, nil
}
