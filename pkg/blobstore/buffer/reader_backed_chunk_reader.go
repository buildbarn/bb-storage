package buffer

import (
	"io"
)

type readerBackedChunkReader struct {
	r                     io.ReadCloser
	maximumChunkSizeBytes int

	buf *[]byte
	err error
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
	if r.err == nil {
		if r.buf == nil {
			r.buf = getChunkBuffer(r.maximumChunkSizeBytes)
		}
		b := *r.buf
		n, err := io.ReadFull(r.r, b)
		if err == io.ErrUnexpectedEOF {
			r.err = io.EOF
		} else {
			r.err = err
		}
		if n > 0 {
			return b[:n], nil
		}
	}
	return nil, r.err
}

func (r *readerBackedChunkReader) Close() {
	if r.buf != nil {
		putChunkBuffer(r.buf)
		r.buf = nil
	}
	r.r.Close()
}
