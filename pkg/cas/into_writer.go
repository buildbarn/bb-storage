package cas

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// IntoWriter streams the chunks of a blob directly into the provided
// io.Writer from the specified offset. The semaphore limits how many
// chunks are read ahead, as described in FetchChunksInOrder().
func IntoWriter(ctx context.Context, chunkBytesReader reader.Reader[[]byte], chunkMappingFetcher chunk.MappingFetcher, readAheadSemaphore *semaphore.Weighted, params *remoteexecution.RepMaxCdcParams, d digest.Digest, offset int64, w io.Writer) error {
	if offset < 0 || offset > d.GetSizeBytes() {
		return status.Errorf(codes.InvalidArgument, "Invalid offset %d for digest %s", offset, d)
	}
	if offset == d.GetSizeBytes() {
		return nil
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

	mapping, err := chunkMappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return err
	}
	index, chunkOffset := mapping.FindChunkOffset(uint64(offset))
	return FetchChunksInOrder(
		ctx,
		mapping.GetDigests(d.GetInstanceName())[index:],
		chunkBytesReader.Read,
		readAheadSemaphore,
		func(chunkData []byte) error {
			_, err := w.Write(chunkData[chunkOffset:])
			chunkOffset = 0
			return err
		},
	)
}
