package util

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// NewZstdReadCloser creates a new io.ReadCloser that wraps an underlying
// reader and decompresses the data using Zstandard. The reader will close
// both the decoder and the underlying reader when it is closed.
func NewZstdReadCloser(underlyingReader io.ReadCloser, options ...zstd.DOption) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(underlyingReader, options...)
	if err != nil {
		return nil, err
	}
	return &zstdReadCloser{Decoder: decoder, underlyingReader: underlyingReader}, nil
}

type zstdReadCloser struct {
	*zstd.Decoder

	underlyingReader io.ReadCloser
}

func (r *zstdReadCloser) Close() error {
	r.Decoder.Close()
	return r.underlyingReader.Close()
}
