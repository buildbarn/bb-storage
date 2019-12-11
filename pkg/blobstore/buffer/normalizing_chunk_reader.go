package buffer

type normalizingChunkReader struct {
	ChunkReader
	maximumChunkSizeBytes int
	lastChunk             []byte
}

// newNormalizingChunkReader creates a decorator for ChunkReader that
// normalizes the sizes of the chunks returned by Read(). It causes
// empty chunks to be omitted. Chunks that exceed a provided maximum
// size are decomposed into smaller ones.
func newNormalizingChunkReader(r ChunkReader, maximumChunkSizeBytes int) ChunkReader {
	return &normalizingChunkReader{
		ChunkReader:           r,
		maximumChunkSizeBytes: maximumChunkSizeBytes,
	}
}

func (r *normalizingChunkReader) Read() ([]byte, error) {
	for {
		if len(r.lastChunk) > 0 {
			if len(r.lastChunk) > r.maximumChunkSizeBytes {
				chunk := r.lastChunk[:r.maximumChunkSizeBytes]
				r.lastChunk = r.lastChunk[r.maximumChunkSizeBytes:]
				return chunk, nil
			}
			chunk := r.lastChunk
			r.lastChunk = nil
			return chunk, nil
		}

		chunk, err := r.ChunkReader.Read()
		if err != nil {
			return nil, err
		}
		r.lastChunk = chunk
	}
}
