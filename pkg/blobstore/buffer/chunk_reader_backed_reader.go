package buffer

import (
	"io"
)

type chunkReaderBackedReader struct {
	r         ChunkReader
	lastChunk []byte
}

func newChunkReaderBackedReader(r ChunkReader) io.ReadCloser {
	return &chunkReaderBackedReader{
		r: r,
	}
}

func (r *chunkReaderBackedReader) Read(p []byte) (int, error) {
	nTotal := copy(p, r.lastChunk)
	p = p[nTotal:]
	r.lastChunk = r.lastChunk[nTotal:]

	for len(p) > 0 {
		chunk, err := r.r.Read()
		if err != nil {
			return nTotal, err
		}
		nCopied := copy(p, chunk)
		nTotal += nCopied
		p = p[nCopied:]
		r.lastChunk = chunk[nCopied:]
	}
	return nTotal, nil
}

func (r *chunkReaderBackedReader) Close() error {
	r.r.Close()
	return nil
}
