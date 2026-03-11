package zstd

import (
	"context"
	"io"
)

// Encoder compresses data written to it using Zstandard. Calling
// Close() flushes the encoder and, for pooled implementations, returns
// the encoder to the pool.
type Encoder interface {
	io.Writer
	Close() error
}

// Decoder decompresses Zstandard-compressed data read from it. Calling
// Close() releases resources and, for pooled implementations, returns
// the decoder to the pool.
type Decoder interface {
	io.Reader
	Close()
}

// Pool manages the lifecycle of ZSTD encoders and decoders. Callers
// must Close() the returned Encoder/Decoder when done.
type Pool interface {
	NewEncoder(ctx context.Context, w io.Writer) (Encoder, error)
	NewDecoder(ctx context.Context, r io.Reader) (Decoder, error)
}
