package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingCDCParametersFetcher struct {
	CDCParametersFetcher
	cache *ttlcache.TTLCache[digest.InstanceName, *remoteexecution.RepMaxCdcParams]
}

// NewCachingCDCParametersFetcher decorates a CDCParametersFetcher with
// caching.
func NewCachingCDCParametersFetcher(base CDCParametersFetcher, ttlCache *ttlcache.TTLCache[digest.InstanceName, *remoteexecution.RepMaxCdcParams]) CDCParametersFetcher {
	return &cachingCDCParametersFetcher{
		CDCParametersFetcher: base,
		cache:                ttlCache,
	}
}

func (f *cachingCDCParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	if params, ok := f.cache.Get(instanceName); ok {
		return params, nil
	}
	params, err := f.CDCParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	f.cache.Put(instanceName, params)
	return params, nil
}
