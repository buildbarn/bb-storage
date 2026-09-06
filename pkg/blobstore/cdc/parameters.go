package cdc

import (
	"context"
	"math"

	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Parameters are the parameters by which to perform Content Defined
// Chunking (CDC).
type Parameters struct {
	MinChunkSizeBytes int64
	HorizonSizeBytes  int64
}

// ParametersFetcher retrieves the Content Defined Chunking (CDC)
// parameters that govern how blobs are decomposed into chunks.
type ParametersFetcher interface {
	FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (Parameters, error)
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

func (f *capabilitiesParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (Parameters, error) {
	capabilities, err := f.provider.GetCapabilities(ctx, instanceName)
	if err != nil {
		return Parameters{}, util.StatusWrap(err, "Unable to GetCapabilities to determine chunking parameters")
	}

	params := capabilities.CacheCapabilities.GetRepMaxCdcParams()
	if params == nil {
		return Parameters{}, status.Error(codes.Unimplemented, "This backend only supports storage backends with RepMaxCDC support")
	}
	if params.MinChunkSizeBytes < 64 {
		return Parameters{}, status.Errorf(codes.Internal, "RepMaxCDC minimum chunk size was %d bytes but a minimum of 64 bytes is required", params.MinChunkSizeBytes)
	}
	if params.MinChunkSizeBytes > math.MaxInt64 || params.HorizonSizeBytes > math.MaxInt64 {
		return Parameters{}, status.Error(codes.Internal, "CDC Parameters could not be represented as int64")
	}

	return Parameters{MinChunkSizeBytes: int64(params.MinChunkSizeBytes), HorizonSizeBytes: int64(params.HorizonSizeBytes)}, nil
}
