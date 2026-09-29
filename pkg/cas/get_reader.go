package cas

import (
	"bytes"
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetReader returns an io.Reader that reads contents of a blob
// seeked to a specific offset.
func GetReader(ctx context.Context, chunkBytesReader reader.Reader[[]byte], chunkMappingFetcher chunk.MappingFetcher, params *remoteexecution.RepMaxCdcParams, d digest.Digest, offset int64) (io.Reader, error) {
	if offset < 0 || offset > d.GetSizeBytes() {
		return nil, status.Errorf(codes.InvalidArgument, "Offset %d is outside of blob of size %s", offset, d)
	}

	if IsSingleChunk(params, d) {
		chunkBytes, err := chunkBytesReader.Read(ctx, d)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(chunkBytes[offset:]), nil
	}

	mapping, err := chunkMappingFetcher.FetchChunkMapping(ctx, d)
	if err != nil {
		return nil, util.StatusWrap(err, "Could not fetch chunk mapping")
	}
	index, chunkOffset := mapping.FindChunkOffset(uint64(offset))
	offsetDigests := mapping.Digests[index:]
	r := chunk.NewReaderFromMapping(ctx, offsetDigests, chunkBytesReader)
	if chunkOffset > 0 {
		if _, err := io.CopyN(io.Discard, r, chunkOffset); err != nil {
			return nil, util.StatusWrap(err, "Failed to skip to read offset")
		}
	}
	return r, nil
}
