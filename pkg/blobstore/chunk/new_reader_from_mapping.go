package chunk

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mappingReadCloser is an io.ReadCloser that stitches together the
// contents of a blob based on an ordered mapping of chunk digests.
type mappingReadCloser struct {
	ctx              context.Context
	chunkBytesReader reader.Reader[[]byte]
	chunkDigests     []digest.Digest

	currentChunkIndex  int
	currentChunkData   []byte
	currentChunkOffset int
	closed             bool
}

// NewReaderFromMapping creates an io.ReadCloser that yields the
// concatenated contents of the chunks identified by the provided
// digests.
func NewReaderFromMapping(ctx context.Context, chunkDigests []digest.Digest, chunkBytesReader reader.Reader[[]byte]) io.ReadCloser {
	return &mappingReadCloser{
		ctx:              ctx,
		chunkBytesReader: chunkBytesReader,
		chunkDigests:     chunkDigests,
	}
}

func (r *mappingReadCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, status.Error(codes.Internal, "Reader is already closed")
	}

	// Fetch the next chunk if the current one is exhausted. Chunk mappings
	// are guaranteed not to contain empty chunks.
	if r.currentChunkOffset >= len(r.currentChunkData) {
		if r.currentChunkIndex >= len(r.chunkDigests) {
			return 0, io.EOF
		}
		chunkDigest := r.chunkDigests[r.currentChunkIndex]
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

func (r *mappingReadCloser) Close() error {
	r.closed = true
	r.currentChunkData = nil
	return nil
}
