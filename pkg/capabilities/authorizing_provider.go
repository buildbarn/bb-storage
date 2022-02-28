package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type authorizingProvider struct {
	base       Provider
	authorizer auth.Authorizer
}

// NewAuthorizingProvider creates a decorator for Provider that only
// performs GetCapabilities() calls in case a client is authorized.
func NewAuthorizingProvider(base Provider, authorizer auth.Authorizer) Provider {
	return &authorizingProvider{
		base:       base,
		authorizer: authorizer,
	}
}

func (p *authorizingProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	if err := auth.AuthorizeSingleInstanceName(ctx, p.authorizer, instanceName); err != nil {
		return nil, util.StatusWrap(err, "Authorization")
	}
	return p.base.GetCapabilities(ctx, instanceName)
}
