package buffer

type offsetChunkReader struct {
	ChunkReader
	prefix []byte
}

// newOffsetChunkReader creates a decorator for ChunkReader that omits
// the beginning of the stream, up to a provided offset.
func newOffsetChunkReader(r ChunkReader, off int64) ChunkReader {
	prefix, err := discardFromChunkReader(r, off)
	if err != nil {
		r.Close()
		return newErrorChunkReader(err)
	}
	if len(prefix) == 0 {
		// Trimmed prefix up to an exact chunk boundary. There
		// is no need to wrap this ChunkReader.
		return r
	}
	return &offsetChunkReader{
		ChunkReader: r,
		prefix:      prefix,
	}
}

func (r *offsetChunkReader) Read() ([]byte, error) {
	if prefix := r.prefix; len(prefix) > 0 {
		r.prefix = nil
		return prefix, nil
	}
	return r.ChunkReader.Read()
}
