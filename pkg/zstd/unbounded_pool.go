package zstd

import (
	"context"
	"io"

	"github.com/klauspost/compress/zstd"
)

type unboundedPool struct {
	encoderOptions []zstd.EOption
	decoderOptions []zstd.DOption
}

// NewUnboundedPool creates a Pool that creates a fresh encoder/decoder
// for each call. This is suitable when memory bounding is not required.
func NewUnboundedPool(encoderOptions []zstd.EOption, decoderOptions []zstd.DOption) Pool {
	return &unboundedPool{
		encoderOptions: encoderOptions,
		decoderOptions: decoderOptions,
	}
}

func (p *unboundedPool) NewEncoder(_ context.Context, w io.Writer) (Encoder, error) {
	return zstd.NewWriter(w, p.encoderOptions...)
}

func (p *unboundedPool) NewDecoder(_ context.Context, r io.Reader) (Decoder, error) {
	dec, err := zstd.NewReader(r, p.decoderOptions...)
	if err != nil {
		return nil, err
	}
	return &unboundedDecoder{Decoder: dec}, nil
}

// unboundedDecoder wraps *zstd.Decoder to implement the Decoder
// interface.
type unboundedDecoder struct {
	*zstd.Decoder
}
