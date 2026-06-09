package cdc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Parameters represents the safely-typed, validated chunking
// boundaries.
type Parameters struct {
	MinChunkSizeBytes int64
	HorizonSizeBytes  int64
}

// GetCDCParameters gets the Content Defined Chunking parameters for a
// given instance name.
func GetCDCParameters(ctx context.Context, capabilitiesProvider capabilities.Provider, instanceName digest.InstanceName) (Parameters, error) {
	capabilities, err := capabilitiesProvider.GetCapabilities(ctx, instanceName)
	if err != nil {
		return Parameters{}, util.StatusWrap(err, "Unable to GetCapabilities to determine chunking parameters")
	}

	params := capabilities.GetCacheCapabilities().GetRepMaxCdcParams()
	if params == nil {
		return Parameters{}, status.Error(codes.Unimplemented, "This backend only supports upstream servers with rep max cdc support.")
	}
	if params.MinChunkSizeBytes < 64 {
		return Parameters{}, status.Errorf(codes.Internal, "MinChunkSizeBytes was %d but a minimum of 64 is required.", params.MinChunkSizeBytes)
	}

	return Parameters{
		MinChunkSizeBytes: int64(params.MinChunkSizeBytes),
		HorizonSizeBytes:  int64(params.HorizonSizeBytes),
	}, nil
}
