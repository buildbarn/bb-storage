package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetBytes returns the bytes of a digest from the CAS as a byteslice.
// An error is returned if the digest is larger than maximumSizeBytes.
func GetBytes(ctx context.Context, chunkBytesReader reader.Reader[[]byte], chunkListFetcher chunklist.Fetcher, params *remoteexecution.RepMaxCdcParams, d digest.Digest, maximumSizeBytes int64) ([]byte, error) {
	if d.GetSizeBytes() > maximumSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Digest size of %d bytes exceeds maximum size of %d bytes", d.GetSizeBytes(), maximumSizeBytes)
	}
	if IsSingleChunk(params, d) {
		return chunkBytesReader.Read(ctx, d)
	}
	manifest, err := chunkListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch chunk list")
	}
	ret := make([]byte, d.GetSizeBytes())
	for i := range manifest.Digests {
		chunkBytes, err := chunkBytesReader.Read(ctx, manifest.Digests[i])
		if err != nil {
			return nil, util.StatusWrap(err, "Could not fetch chunk")
		}
		copy(ret[manifest.Offsets[i]:], chunkBytes)
	}
	return ret, nil
}
