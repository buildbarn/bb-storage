package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/proto"
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

	capabilities, err := s.provider.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	// TODO: Instead of setting these version numbers here, should
	// we let providers set these and have MergingProvider merge
	// those as well?
	capabilitiesWithVersion := remoteexecution.ServerCapabilities{
		DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 0},
		LowApiVersion:        &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion:       &semver.SemVer{Major: 2, Minor: 3},
	}
	proto.Merge(&capabilitiesWithVersion, capabilities)
	return &capabilitiesWithVersion, nil
}
