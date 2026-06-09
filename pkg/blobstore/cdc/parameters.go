package cdc

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ParametersFetcher retrieves the Content Defined Chunking (CDC)
// parameters that govern how blobs are decomposed into chunks. Unless
// an error is returned, the returned value is guaranteed to be non-nil
// and to have been validated, so that callers may rely on its values
// without performing any checks.
type ParametersFetcher interface {
	FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error)
}

type capabilitiesParametersFetcher struct {
	provider capabilities.Provider
}

// NewCapabilitiesParametersFetcher creates a ParametersFetcher that
// obtains the CDC parameters by calling GetCapabilities() on the
// provided capabilities.Provider.
func NewCapabilitiesParametersFetcher(provider capabilities.Provider) ParametersFetcher {
	return &capabilitiesParametersFetcher{
		provider: provider,
	}
}

func (f *capabilitiesParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	capabilities, err := f.provider.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrap(err, "Unable to GetCapabilities to determine chunking parameters")
	}

	params := capabilities.CacheCapabilities.GetRepMaxCdcParams()
	if params == nil {
		return nil, status.Error(codes.Unimplemented, "This backend only supports storage backends with RepMaxCDC support")
	}
	return params, nil
}
