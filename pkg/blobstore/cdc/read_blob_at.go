package cdc

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReadBlobAt fills buf with the contents of a blob starting at the
// given offset, following the semantics of io.ReaderAt: it returns
// the number of bytes read and io.EOF when the read reaches the end
// of the blob before filling buf completely.
//
// Blobs stored as a single chunk are read with a single Chunk Storage
// fetch. For larger blobs only the chunks covering the requested
// range are fetched, so random access reads do not stream the full
// blob.
func ReadBlobAt(ctx context.Context, cas ContentAddressableStorage, d digest.Digest, buf []byte, offset int64) (int, error) {
	if offset < 0 || offset > d.GetSizeBytes() {
		return 0, status.Errorf(codes.InvalidArgument, "Offset %d is outside of blob %s", offset, d)
	}
	if len(buf) == 0 {
		return 0, nil
	}

	params, err := cas.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return 0, util.StatusWrap(err, "Could not fetch CDC parameters")
	}
	if IsSingleChunk(params, d) {
		chunk, err := cas.FetchChunk(ctx, d)
		if err != nil {
			return 0, err
		}
		n := copy(buf, chunk[offset:])
		if n < len(buf) {
			return n, io.EOF
		}
		return n, nil
	}

	manifest, err := cas.GetManifest(ctx, d)
	if err != nil {
		return 0, util.StatusWrap(err, "Could not fetch chunk list")
	}
	index, chunkOffset := chunklist.FindChunkOffset(manifest, uint64(offset))

	n := 0
	for n < len(buf) && index < len(manifest) {
		chunkBytes, err := cas.FetchChunk(ctx, manifest[index].Digest)
		if err != nil {
			return n, util.StatusWrap(err, "Could not fetch chunk")
		}
		n += copy(buf[n:], chunkBytes[chunkOffset:])
		chunkOffset = 0
		index++
	}
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}
