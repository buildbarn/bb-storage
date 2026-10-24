package coder

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
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
	c, err := chunk.NewChunkCompressed(data, int(d.GetSizeBytes()))
	if err != nil {
		return nil, err
	}

	if d2 := c.ComputeDigest(d.GetDigestFunction()); d2 != d {
		return nil, status.Errorf(codes.InvalidArgument, "Digest mismatch, expected %s, got %s", d, d2)
	}
	return c, nil
}
