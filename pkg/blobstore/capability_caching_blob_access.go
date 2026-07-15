package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type capabilitiesCachingBlobAccess struct {
	BlobAccess
	capabilitiesCache *TTLCache[*remoteexecution.ServerCapabilities]
	fetcher           func(context.Context, string) (*remoteexecution.ServerCapabilities, error)
}

// NewCapabilitiesCachingBlobAccess creates a decorator for BlobAccess
// that adds caching to the GetCapabilities() operation using a
// TTLCache.
//
// This is particularly useful for clients that need to frequently query
// the capapbilities for e.g. getting the cdc parameters.
func NewCapabilitiesCachingBlobAccess(base BlobAccess, capabilitiesCache *TTLCache[*remoteexecution.ServerCapabilities]) BlobAccess {
	return &capabilitiesCachingBlobAccess{
		BlobAccess:        base,
		capabilitiesCache: capabilitiesCache,
		fetcher: func(ctx context.Context, instanceName string) (*remoteexecution.ServerCapabilities, error) {
			i, err := digest.NewInstanceName(instanceName)
			if err != nil {
				return nil, err
			}
			return base.GetCapabilities(ctx, i)
		},
	}
}

func (ba *capabilitiesCachingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return ba.capabilitiesCache.GetOrSet(ctx, instanceName.String(), ba.fetcher)
}
