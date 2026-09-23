package coder

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkCoder struct {
	zstdPool         zstd.Pool
	encodeCompressed bool
}

// NewChunkCoder returns a Coder that can encode and decode a
// *chunk.Chunk into an efficient binary format.
func NewChunkCoder(zstdPool zstd.Pool) Coder[*chunk.Chunk, []byte] {
	return &chunkCoder{
		zstdPool: zstdPool,
	}
}

func (chunkCoder) checkDigest(ctx context.Context, chunk *chunk.Chunk, d digest.Digest) error {
	generator := d.GetDigestFunction().NewGenerator(d.GetSizeBytes())
	bytes, err := chunk.GetBytes(ctx)
	if err != nil {
		return err
	}
	if _, err := generator.Write(bytes); err != nil {
		return err
	}
	d2 := generator.Sum()
	if d2 != d {
		return status.Errorf(codes.InvalidArgument, "Digest mismatch, expected %s, got %s", d, d2)
	}
	return nil
}

func (c *chunkCoder) Encode(chunk *chunk.Chunk, d digest.Digest) ([]byte, error) {
	// TODO: Should ctx be part of this signature?
	ctx := context.Background()
	if err := c.checkDigest(ctx, chunk, d); err != nil {
		return nil, err
	}
	return chunk.GetBytesCompressed(ctx)
}

func (c *chunkCoder) Decode(data []byte, d digest.Digest) (*chunk.Chunk, error) {
	ctx := context.Background()
	chunk := chunk.NewChunkFromCompressedData(c.zstdPool, data)
	if err := c.checkDigest(ctx, chunk, d); err != nil {
		return nil, err
	}
	return chunk, nil
}
