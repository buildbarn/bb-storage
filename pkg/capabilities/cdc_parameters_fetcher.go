package capabilities

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CDCParametersFetcher retrieves the Content Defined Chunking (CDC)
// parameters that govern how blobs are decomposed into chunks. Unless
// an error is returned, the returned value is guaranteed to be non-nil
// and to have been validated, so that callers may rely on its values
// without performing any checks.
type CDCParametersFetcher interface {
	FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error)
}

type baseCDCParametersFetcher struct {
	provider Provider
}

// NewBaseCDCParametersFetcher creates a CDCParametersFetcher that
// obtains the CDC parameters by calling GetCapabilities() on the
// provided capabilities.Provider.
func NewBaseCDCParametersFetcher(provider Provider) CDCParametersFetcher {
	return &baseCDCParametersFetcher{
		provider: provider,
	}
}

func (f *baseCDCParametersFetcher) FetchCDCParameters(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.RepMaxCdcParams, error) {
	capabilities, err := f.provider.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrap(err, "Unable to GetCapabilities to determine chunking parameters")
	}

	params := capabilities.CacheCapabilities.GetRepMaxCdcParams()
	if params == nil {
		return nil, status.Error(codes.Unimplemented, "This backend only supports storage backends with RepMaxCDC support")
	}
	if params.MinChunkSizeBytes < 64 {
		return nil, status.Errorf(codes.Internal, "Upstream server advertises a RepMaxCDC minimum chunk size of %d bytes, but a minimum of 64 bytes is required", params.MinChunkSizeBytes)
	}
	if params.MinChunkSizeBytes > LargestAcceptableMinChunkSizeBytes {
		return nil, status.Errorf(codes.Internal, "Upstream server advertises a RepMaxCDC minimum chunk size of %d bytes, more than the maximum of %d bytes this server accepts", params.MinChunkSizeBytes, LargestAcceptableMinChunkSizeBytes)
	}
	if params.HorizonSizeBytes > LargestAcceptableHorizonSizeBytes {
		return nil, status.Errorf(codes.Internal, "Upstream server advertises a RepMaxCDC horizon size of %d bytes, more than the maximum of %d bytes this server accepts", params.HorizonSizeBytes, LargestAcceptableHorizonSizeBytes)
	}
	return params, nil
}

// LargestAcceptableMinChunkSizeBytes is the largest minimum chunk size
// this server accepts.
const LargestAcceptableMinChunkSizeBytes = 16 << 20 // 16 MiB

// LargestAcceptableHorizonSizeBytes is the largest horizon size this
// server accepts.
const LargestAcceptableHorizonSizeBytes = 128 << 20 // 128 MiB
