package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type updateEnabledTogglingBuildQueue struct {
	BuildQueue

	updateEnabledForInstanceName digest.InstanceNameMatcher
}

// NewUpdateEnabledTogglingBuildQueue alters the response of
// GetCapabilities() to announce whether this build queue allows direct
// writing to the Action Cache. It does this by toggling the
// UpdateEnabled flag.
func NewUpdateEnabledTogglingBuildQueue(base BuildQueue, updateEnabledForInstanceName digest.InstanceNameMatcher) BuildQueue {
	return &updateEnabledTogglingBuildQueue{
		BuildQueue:                   base,
		updateEnabledForInstanceName: updateEnabledForInstanceName,
	}
}

func (bq *updateEnabledTogglingBuildQueue) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}

	// Extract underlying capabilities.
	oldCapabilities, err := bq.BuildQueue.GetCapabilities(ctx, in)
	if err != nil {
		return nil, err
	}

	// If CacheCapabilities are provided, alter them to announce
	// that the Action Cache permits updates.
	newCapabilities := *oldCapabilities
	if oldCacheCapabilities := newCapabilities.CacheCapabilities; oldCacheCapabilities != nil {
		newCacheCapabilities := *oldCacheCapabilities
		newCapabilities.CacheCapabilities = &newCacheCapabilities
		newCacheCapabilities.ActionCacheUpdateCapabilities = &remoteexecution.ActionCacheUpdateCapabilities{
			UpdateEnabled: bq.updateEnabledForInstanceName(instanceName),
		}
	}
	return &newCapabilities, nil
}
