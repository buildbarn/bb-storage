package chunklistvalidating

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Chunk is a struct of raw binary data with its digest.
type Chunk struct {
	Digest digest.Digest
	Data   []byte
}

// Chunker is an interface that provides a sequence of chunks.
type Chunker interface {
	NextChunk() (Chunk, error)
}
