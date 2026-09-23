package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type chunkBytesReader struct {
	chunkStorage blobstore.BlobAccess[*chunk.Chunk]
}

// NewChunkBytesReader creates a Reader that returns the decompressed
// bytes of chunks fetched from a Chunk Storage (CS).
func NewChunkBytesReader(chunkStorage blobstore.BlobAccess[*chunk.Chunk]) reader.Reader[[]byte] {
	return &chunkBytesReader{
		chunkStorage: chunkStorage,
	}
}

func (r *chunkBytesReader) Read(ctx context.Context, d digest.Digest) ([]byte, error) {
	chunk, err := r.chunkStorage.Get(ctx, d)
	if err != nil {
		return nil, err
	}
	return chunk.GetBytes(ctx)
}
