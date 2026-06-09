package coder

import (
	"bytes"
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
)

const (
	// zstdEncodeShrinkDivisor is the divisor used for the initial
	// buffer for zstd encoding. It reduces the number of micro
	// allocations by setting an initial size of the buffer close to
	// what could be expected.
	zstdEncodeShrinkDivisor = 10

	// zstdDecodeGrowthFactor is the factor used for the initial buffer
	// for zstd decoding. It reduces the number of micro allocations by
	// setting an initial size of the buffer close to what could be
	// expected.
	zstdDecodeGrowthFactor = 1

	// zstdMinimumBufferSize prevents very small initial buffers from
	// being used. A value of 64 corresponds to the value used natively
	// by bytes.Buffer.
	zstdMinimumBufferSize = 64
)

type zstdCoder struct {
	pool bb_zstd.Pool
}

// NewZSTDCoder creates a Coder that codes byte slices using a
// zstd.Pool.
func NewZSTDCoder(pool bb_zstd.Pool) Coder[[]byte, []byte] {
	return &zstdCoder{
		pool: pool,
	}
}

func (c *zstdCoder) Encode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	var buf bytes.Buffer
	// TODO: Should this context be takes somewhere else? Make the coder
	// take a context or maybe supplied in the constructor?
	ctx := context.Background()
	estimatedSize := len(data) / zstdEncodeShrinkDivisor
	if estimatedSize < zstdMinimumBufferSize {
		estimatedSize = zstdMinimumBufferSize
	}
	buf.Grow(estimatedSize)
	encoder, err := c.pool.NewEncoder(ctx, &buf)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to get encoder from pool")
	}
	if _, err := encoder.Write(data); err != nil {
		encoder.Close()
		return nil, util.StatusWrap(err, "Failed to compress data")
	}
	if err := encoder.Close(); err != nil {
		return nil, util.StatusWrap(err, "Failed to finish compression of data")
	}
	return buf.Bytes(), nil
}

func (c *zstdCoder) Decode(data []byte, parentDigest digest.Digest) ([]byte, error) {
	decoder, err := c.pool.NewDecoder(context.Background(), bytes.NewReader(data))
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to get decoder from pool")
	}
	defer decoder.Close()
	var buf bytes.Buffer
	estimatedSize := len(data) * zstdDecodeGrowthFactor
	if estimatedSize < zstdMinimumBufferSize {
		estimatedSize = zstdMinimumBufferSize
	}
	buf.Grow(estimatedSize)
	if _, err := buf.ReadFrom(decoder); err != nil {
		return nil, util.StatusWrap(err, "Failed to decompress data")
	}
	return buf.Bytes(), nil
}
