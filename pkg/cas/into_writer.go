package cas

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IntoWriter streams the chunks of a blob directly into the provided
// io.Writer from the specified offset.
func IntoWriter(ctx context.Context, chunkBytesReader reader.Reader[[]byte], chunkListFetcher chunk.ListFetcher, params *remoteexecution.RepMaxCdcParams, d digest.Digest, offset int64, w io.Writer) error {
	if offset < 0 || offset > d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "Invalid offset %d for digest %s", offset, d)
	}

	if IsSingleChunk(params, d) {
		chunkData, err := chunkBytesReader.Read(ctx, d)
		if err != nil {
			return err
		}
		if _, err := w.Write(chunkData[offset:]); err != nil {
			return err
		}
		return nil
	}

	manifest, err := chunkListFetcher.FetchChunkList(ctx, d)
	if err != nil {
		return err
	}
	index, chunkOffset := manifest.FindChunkOffset(uint64(offset))
	for ; index < len(manifest.Digests); index++ {
		chunkData, err := chunkBytesReader.Read(ctx, manifest.Digests[index])
		if err != nil {
			return err
		}

		if _, err := w.Write(chunkData[chunkOffset:]); err != nil {
			return err
		}
		chunkOffset = 0
	}

	return nil
}
