package cas

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PutReader inserts all chunks for a digest from an io.Reader.
func PutReader(ctx context.Context, zstdPool zstd.Pool, chunkStorage blobstore.BlobAccess[*buffer.Chunk], chunkListStorage blobstore.BlobAccess[chunklist.ChunkList], params *remoteexecution.RepMaxCdcParams, d digest.Digest, r io.Reader) error {
	digestFunction := d.GetDigestFunction()
	chunker := cdc.NewReaderChunker(digestFunction, r, int64(params.MinChunkSizeBytes), int64(params.HorizonSizeBytes))
	wholeGen := digestFunction.NewGenerator(d.GetSizeBytes())

	chunkList := chunklist.ChunkList{
		Digests:   make([]digest.Digest, 0),
		Offsets:   make([]uint64, 0),
		Validated: true,
	}
	var offset uint64
	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return util.StatusWrap(err, "Failed to chunk write stream")
		}

		if _, err := wholeGen.Write(chunk.Data); err != nil {
			return status.Error(codes.Internal, "Could not compute digest of blob")
		}

		if err := chunkStorage.Put(ctx, chunk.Digest, buffer.NewChunk(zstdPool, chunk.Data)); err != nil {
			return util.StatusWrap(err, "Failed to save chunk")
		}

		chunkList.Digests = append(chunkList.Digests, chunk.Digest)
		chunkList.Offsets = append(chunkList.Offsets, offset)
		offset += uint64(chunk.Digest.GetSizeBytes())
		if offset > uint64(d.GetSizeBytes()) {
			return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, digest is supposed to be %d bytes but have already received %d bytes", d.GetSizeBytes(), offset)
		}
	}

	// Verify the whole blob against the advertised digest.
	if actual := wholeGen.Sum(); actual != d {
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch: advertised %s, actual %s", d, actual)
	}

	// A single chunk is the trivial case: it already lives in the
	// chunk storage and needs no chunk list.
	if len(chunkList.Digests) <= 1 {
		return nil
	}

	if err := chunkListStorage.Put(ctx, d, chunkList); err != nil {
		return util.StatusWrap(err, "Could not save chunk list for blob")
	}
	return nil
}
