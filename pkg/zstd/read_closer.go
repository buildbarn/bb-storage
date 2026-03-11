package zstd

import (
	"context"
	"io"
)

// NewReadCloser creates a new io.ReadCloser that decompresses data
// using a decoder from the provided Pool. Closing the returned reader
// releases the decoder back to the pool and closes the underlying
// reader.
func NewReadCloser(ctx context.Context, pool Pool, underlyingReader io.ReadCloser) (io.ReadCloser, error) {
	decoder, err := pool.NewDecoder(ctx, underlyingReader)
	if err != nil {
		return nil, err
	}
	return &readCloser{decoder: decoder, underlyingReader: underlyingReader}, nil
}

type readCloser struct {
	decoder          Decoder
	underlyingReader io.ReadCloser
}

func (r *readCloser) Read(p []byte) (int, error) {
	return r.decoder.Read(p)
}

func (r *readCloser) Close() error {
	r.decoder.Close()
	return r.underlyingReader.Close()
}
