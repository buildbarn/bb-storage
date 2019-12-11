package buffer

import (
	"io"
)

type readerBackedChunkReader struct {
	r                     io.ReadCloser
	maximumChunkSizeBytes int
}

// newReaderBackedChunkReader creates a ChunkReader based on an existing
// ReadCloser. It attempts to read data from the ReadCloser, turning it
// into chunks of the maximum permitted size.
func newReaderBackedChunkReader(r io.ReadCloser, maximumChunkSizeBytes int) ChunkReader {
	return &readerBackedChunkReader{
		r:                     r,
		maximumChunkSizeBytes: maximumChunkSizeBytes,
	}
}

func (r *readerBackedChunkReader) Read() ([]byte, error) {
	b := make([]byte, r.maximumChunkSizeBytes)
	n, err := io.ReadFull(r.r, b[:])
	if n > 0 {
		return b[:n], nil
	}
	if err == io.ErrUnexpectedEOF {
		return nil, io.EOF
	}
	return nil, err
}

func (r *readerBackedChunkReader) Close() {
	r.r.Close()
}
