package coder

import (
	"bytes"
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type chunkCoder struct {
	zstdPool zstd.Pool
}

// NewChunkCoder returns a Coder that can encode and decode a
// *chunk.Chunk into an efficient binary format.
func NewChunkCoder(zstdPool zstd.Pool) Coder[*chunk.Chunk, []byte] {
	return &chunkCoder{
		zstdPool: zstdPool,
	}
}

func (chunkCoder) Encode(chunk *chunk.Chunk, d digest.Digest) ([]byte, error) {
	// TODO: Should ctx be part of this signature?
	ctx := context.Background()
	return chunk.GetBytesCompressed(ctx)
}

func (c *chunkCoder) Decode(data []byte, d digest.Digest) (*chunk.Chunk, error) {
	ctx := context.Background()

	decoder, err := c.zstdPool.NewDecoder(ctx, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	decompressed, err := util.ReadFullyDecompressed(decoder, d.GetSizeBytes())
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
