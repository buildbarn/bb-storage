package coder

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
	return chunk.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
}

func (chunkCoder) Decode(data []byte, d digest.Digest) (*chunk.Chunk, error) {
	c, err := chunk.NewChunkFromEncoded(data, int(d.GetSizeBytes()), remoteexecution.Compressor_ZSTD)
	if err != nil {
		return nil, err
	}

	if d2 := c.ComputeDigest(d.GetDigestFunction()); d2 != d {
		return nil, status.Errorf(codes.InvalidArgument, "Digest mismatch, expected %s, got %s", d, d2)
	}
	return c, nil
}
