package builder

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type updateEnabledTogglingBuildQueue struct {
	BuildQueue

	authorizer auth.Authorizer
}

// NewUpdateEnabledTogglingBuildQueue alters the response of
// GetCapabilities() to announce whether this build queue allows direct
// writing to the Action Cache. It does this by toggling the
// UpdateEnabled flag.
func NewUpdateEnabledTogglingBuildQueue(base BuildQueue, authorizer auth.Authorizer) BuildQueue {
	return &updateEnabledTogglingBuildQueue{
		BuildQueue: base,
		authorizer: authorizer,
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
		updateEnabled := true
		switch err := auth.AuthorizeSingleInstanceName(ctx, bq.authorizer, instanceName); status.Code(err) {
		case codes.OK:
			// Nothing to do.
		case codes.PermissionDenied:
			updateEnabled = false
		default:
			return nil, err
		}

		newCacheCapabilities := *oldCacheCapabilities
		newCapabilities.CacheCapabilities = &newCacheCapabilities
		newCacheCapabilities.ActionCacheUpdateCapabilities = &remoteexecution.ActionCacheUpdateCapabilities{
			UpdateEnabled: updateEnabled,
		}
	}
	return &newCapabilities, nil
}
