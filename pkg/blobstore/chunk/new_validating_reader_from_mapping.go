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

type validatingMappingReader struct {
	ctx              context.Context
	chunkBytesReader reader.Reader[[]byte]
	chunkDigests     []digest.Digest
	blobDigest       digest.Digest
	digestGenerator  *digest.Generator

	currentChunkIndex  int
	currentChunkData   []byte
	currentChunkOffset int
	err                error
}

// NewValidatingReaderFromMapping creates an io.Reader that yields the
// concatenated contents of the chunks identified by the provided
// digests. The chunks are required to concatenate to the blob digest,
// which is verified upon the final chunk of the mapping being fetched.
func NewValidatingReaderFromMapping(ctx context.Context, chunkDigests []digest.Digest, d digest.Digest, chunkBytesReader reader.Reader[[]byte]) io.Reader {
	return &validatingMappingReader{
		ctx:              ctx,
		chunkBytesReader: chunkBytesReader,
		chunkDigests:     chunkDigests,
		blobDigest:       d,
		digestGenerator:  d.GetDigestFunction().NewGenerator(d.GetSizeBytes()),
	}
}

func (r *validatingMappingReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	// Fetch the next chunk if the current one is exhausted.
	if r.currentChunkOffset >= len(r.currentChunkData) {
		if r.currentChunkIndex >= len(r.chunkDigests) {
			return 0, io.EOF
		}
		chunkDigest := r.chunkDigests[r.currentChunkIndex]
		chunkData, err := r.chunkBytesReader.Read(r.ctx, chunkDigest)
		if err != nil {
			r.err = util.StatusWrapf(err, "Failed to fetch chunk at index %d", r.currentChunkIndex)
			return 0, r.err
		}
		if _, err := r.digestGenerator.Write(chunkData); err != nil {
			r.err = status.Error(codes.Internal, "Failed to compute digest of blob")
			return 0, r.err
		}
		r.currentChunkData = chunkData
		r.currentChunkOffset = 0
		r.currentChunkIndex++
		if r.currentChunkIndex == len(r.chunkDigests) {
			// The final chunk of the mapping was fetched. At this point
			// we can verify the digest.
			if actual := r.digestGenerator.Sum(); actual != r.blobDigest {
				r.err = status.Errorf(
					codes.Internal,
					"Blob digest mismatch, advertised %s, actual %s",
					r.blobDigest,
					actual,
				)
				// The reader is useless now.
				return 0, r.err
			}
		}
	}

	// Copy as much data as available from the current chunk into p.
	n := copy(p, r.currentChunkData[r.currentChunkOffset:])
	r.currentChunkOffset += n

	if r.currentChunkOffset >= len(r.currentChunkData) {
		r.currentChunkData = nil
	}

	return n, nil
}
