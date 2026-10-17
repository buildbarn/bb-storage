package coder

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkCoder struct{}

// NewChunkCoder returns a Coder that can encode and decode a
// *chunk.Chunk into an efficient binary format.
func NewChunkCoder() Coder[*chunk.Chunk, []byte] {
	return chunkCoder{}
}

func (chunkCoder) Encode(chunk *chunk.Chunk, d digest.Digest) ([]byte, error) {
	return chunk.GetBytesCompressed(), nil
}

func (chunkCoder) Decode(data []byte, d digest.Digest) (*chunk.Chunk, error) {
	decompressed, err := zstd.Decode(data, int(d.GetSizeBytes()))
	if err != nil {
		return nil, err
	}

	generator := d.GetDigestFunction().NewGenerator(d.GetSizeBytes())
	if _, err := generator.Write(decompressed); err != nil {
		return nil, err
	}
	d2 := generator.Sum()
	if d2 != d {
		return nil, status.Errorf(codes.InvalidArgument, "Digest mismatch, expected %s, got %s", d, d2)
	}
	return chunk.NewChunkWithCompressedData(decompressed, data), nil
}
