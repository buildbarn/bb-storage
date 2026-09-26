package chunk

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// mappingReader is an io.Reader that stitches together the
// contents of a blob based on an ordered mapping of chunk digests.
type mappingReader struct {
	ctx              context.Context
	chunkBytesReader reader.Reader[[]byte]
	chunkDigests     []digest.Digest

	currentChunkIndex  int
	currentChunkData   []byte
	currentChunkOffset int
}

// NewReaderFromMapping creates an io.Reader that yields the
// concatenated contents of the chunks identified by the provided
// digests.
func NewReaderFromMapping(ctx context.Context, chunkDigests []digest.Digest, chunkBytesReader reader.Reader[[]byte]) io.Reader {
	return &mappingReader{
		ctx:              ctx,
		chunkBytesReader: chunkBytesReader,
		chunkDigests:     chunkDigests,
	}
}

func (r *mappingReader) Read(p []byte) (int, error) {
	// Fetch the next chunk if the current one is exhausted.
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
