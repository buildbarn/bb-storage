package chunklist

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chunkConcatenatingReadCloser is an io.ReadCloser that stitches
// together the contents of a blob based on a ChunkList.
type chunkConcatenatingReadCloser struct {
	ctx              context.Context
	chunkBytesReader reader.Reader[[]byte]
	chunkList        ChunkList

	currentChunkIndex  int
	currentChunkData   []byte
	currentChunkOffset int
	closed             bool
}

// NewChunkConcatenatingReader creates an io.ReadCloser that yields the
// concatenated contents of the chunks in a ChunkList.
func NewChunkConcatenatingReader(ctx context.Context, chunkList ChunkList, chunkBytesReader reader.Reader[[]byte]) io.ReadCloser {
	return &chunkConcatenatingReadCloser{
		ctx:              ctx,
		chunkBytesReader: chunkBytesReader,
		chunkList:        chunkList,
	}
}

func (r *chunkConcatenatingReadCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, status.Error(codes.Internal, "Reader is already closed")
	}

	// Advance to the next chunk if the current one is exhausted,
	// skipping any zero-length chunks.
	for r.currentChunkOffset >= len(r.currentChunkData) {
		if r.currentChunkIndex >= len(r.chunkList.Digests) {
			return 0, io.EOF
		}

		chunkDigest := r.chunkList.Digests[r.currentChunkIndex]
		chunkData, err := r.chunkBytesReader.Read(r.ctx, chunkDigest)
		if err != nil {
			return 0, util.StatusWrapf(err, "Failed to fetch chunk at index %d", r.currentChunkIndex)
		}
		r.currentChunkData = chunkData
		r.currentChunkOffset = 0
		r.currentChunkIndex++
	}

	// Copy as much data as available from the current chunk into p.
	n := copy(p, r.currentChunkData[r.currentChunkOffset:])
	r.currentChunkOffset += n

	if r.currentChunkOffset >= len(r.currentChunkData) {
		r.currentChunkData = nil
	}

	return n, nil
}

func (r *chunkConcatenatingReadCloser) Close() error {
	r.closed = true
	r.currentChunkData = nil
	return nil
}
