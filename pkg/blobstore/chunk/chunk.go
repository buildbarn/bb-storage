package chunk

import (
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/zstd"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed. The uncompressed data of a chunk
// is always available, as creators of chunks are required to decompress
// and verify the chunk against its digest prior to creation. Chunk
// objects are thread safe but not trivially copyable, refer to them by
// pointer.
type Chunk struct {
	data []byte

	compressedData atomic.Pointer[[]byte]
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
func (c *Chunk) GetBytes() []byte {
	return c.data
}

// GetBytesCompressed gets the underlying bytes of the chunk in ZSTD
// compressed format.
func (c *Chunk) GetBytesCompressed() []byte {
	if compressedData := c.compressedData.Load(); compressedData != nil {
		return *compressedData
	}
	compressedData := zstd.Encode(c.data)
	c.compressedData.Store(&compressedData)
	return compressedData
}

// NewChunk creates a chunk from an uncompressed byte slice. Ownership
// of the provided slice is transferred to the chunk, callers must not
// modify or reuse it afterwards.
func NewChunk(data []byte) *Chunk {
	return &Chunk{data: data}
}

// NewChunkWithCompressedData creates a chunk that has data in both its
// compressed and uncompressed form. It is the responsibility of the
// caller to make sure that this chunk is valid. Ownership of the
// provided slices are transferred to the chunk, callers must not modify
// or reuse them afterwards.
func NewChunkWithCompressedData(data, compressedData []byte) *Chunk {
	c := &Chunk{data: data}
	c.compressedData.Store(&compressedData)
	return c
}
