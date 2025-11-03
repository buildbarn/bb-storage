package buffer

type errorChunkReader struct {
	err error
}

// newErrorChunkReader creates a ChunkReader that always returns a fixed
// error response for Read() operations.
func newErrorChunkReader(err error) ChunkReader {
	return errorChunkReader{err: err}
}

func (r errorChunkReader) Read() ([]byte, error) {
	return nil, r.err
}

func (errorChunkReader) Close() {
}
