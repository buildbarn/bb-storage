package cas

import (
	"bytes"
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PutBytes inserts all chunks for a digest from it's raw bytes.
func PutBytes(ctx context.Context, zstdPool zstd.Pool, chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkListStorage blobstore.BlobAccess[chunk.List], params *remoteexecution.RepMaxCdcParams, d digest.Digest, data []byte) error {
	if IsSingleChunk(params, d) {
		// Verify the blob against the advertised digest.
		generator := d.GetDigestFunction().NewGenerator(d.GetSizeBytes())
		if _, err := generator.Write(data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of blob")
		}
		if actual := generator.Sum(); actual != d {
			return status.Errorf(codes.InvalidArgument, "Blob digest mismatch: advertised %s, actual %s", d, actual)
		}
		return chunkStorage.Put(ctx, d, chunk.NewChunk(zstdPool, data))
	}
	return PutReader(ctx, zstdPool, chunkStorage, chunkListStorage, params, d, bytes.NewReader(data))
}
