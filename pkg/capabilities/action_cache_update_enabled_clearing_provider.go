package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type actionCacheUpdateEnabledClearingProvider struct {
	base       Provider
	authorizer auth.Authorizer
}

// NewActionCacheUpdateEnabledClearingProvider creates a decorator for a
// capabilities provider that clears the
// ActionCacheUpdateCapabilities.update_enabled field based on an
// authorization decision. This can be used to report to clients that an
// Action Cache is only available for reading; not for writing.
func NewActionCacheUpdateEnabledClearingProvider(base Provider, authorizer auth.Authorizer) Provider {
	return &actionCacheUpdateEnabledClearingProvider{
		base:       base,
		authorizer: authorizer,
	}
}

func (p *actionCacheUpdateEnabledClearingProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := p.base.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	if serverCapabilities.CacheCapabilities.GetActionCacheUpdateCapabilities().GetUpdateEnabled() {
		if err := auth.AuthorizeSingleInstanceName(ctx, p.authorizer, instanceName); err != nil {
			if status.Code(err) == codes.PermissionDenied {
				// Clear the 'update_enabled' field in the response.
				var copiedCapabilities remoteexecution.ServerCapabilities
				proto.Merge(&copiedCapabilities, serverCapabilities)
				copiedCapabilities.CacheCapabilities.ActionCacheUpdateCapabilities.UpdateEnabled = false
				return &copiedCapabilities, nil
			}
			return nil, util.StatusWrap(err, "Authorization")
		}
	}

	return serverCapabilities, nil
}
