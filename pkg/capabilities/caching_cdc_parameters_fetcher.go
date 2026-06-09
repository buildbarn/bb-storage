package capabilities

import (
	"context"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/lossymap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingCDCParametersFetcher struct {
	CDCParametersFetcher
	clock clock.Clock
	cache lossymap.Map[digest.InstanceName, *remoteexecution.RepMaxCdcParams, time.Time]
	lock  sync.Mutex
}

// NewCachingCDCParametersFetcher decorates a CDCParametersFetcher with
// caching.
func NewCachingCDCParametersFetcher(base CDCParametersFetcher, clock clock.Clock, cache lossymap.Map[digest.InstanceName, *remoteexecution.RepMaxCdcParams, time.Time]) CDCParametersFetcher {
	return &cachingCDCParametersFetcher{
		CDCParametersFetcher: base,
		clock:                clock,
		cache:                cache,
	}
}

func (f *cachingCDCParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	f.lock.Lock()
	params, err := f.cache.Get(instanceName, f.clock.Now())
	f.lock.Unlock()
	if err == nil {
		return params, nil
	} else if status.Code(err) != codes.NotFound {
		return nil, err
	}
	params, err = f.CDCParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	f.lock.Lock()
	err = f.cache.Put(instanceName, params, f.clock.Now())
	f.lock.Unlock()
	if err != nil {
		return nil, err
	}
	return params, nil
}
