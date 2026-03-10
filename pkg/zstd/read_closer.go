package zstd

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

// NewReadCloser creates a new io.ReadCloser that wraps an underlying
// reader and decompresses the data using Zstandard. The reader will
// close both the decoder and the underlying reader when it is closed.
func NewReadCloser(underlyingReader io.ReadCloser, options ...zstd.DOption) (io.ReadCloser, error) {
	decoder, err := zstd.NewReader(underlyingReader, options...)
	if err != nil {
		return nil, err
	}
	return &readCloser{Decoder: decoder, underlyingReader: underlyingReader}, nil
}

type readCloser struct {
	*zstd.Decoder

	underlyingReader io.ReadCloser
}

func (r *readCloser) Close() error {
	r.Decoder.Close()
	return r.underlyingReader.Close()
}
