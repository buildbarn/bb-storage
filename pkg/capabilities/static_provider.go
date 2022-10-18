package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type staticProvider struct {
	capabilities *remoteexecution.ServerCapabilities
}

// NewStaticProvider creates a capabilities provider that returns a
// fixed response. This can be used by individual subsystems to declare
// base providers that report capabilities of features they implement.
func NewStaticProvider(capabilities *remoteexecution.ServerCapabilities) Provider {
	return &staticProvider{
		capabilities: capabilities,
	}
}

func (p *staticProvider) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return p.capabilities, nil
}
