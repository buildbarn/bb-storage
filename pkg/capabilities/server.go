package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type server struct {
	provider Provider
}

// NewServer creates a gRPC server object for the REv2 Capabilities service.
func NewServer(provider Provider) remoteexecution.CapabilitiesServer {
	return &server{
		provider: provider,
	}
}

func (s *server) GetCapabilities(ctx context.Context, in *remoteexecution.GetCapabilitiesRequest) (*remoteexecution.ServerCapabilities, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	return s.provider.GetCapabilities(ctx, instanceName)
}
