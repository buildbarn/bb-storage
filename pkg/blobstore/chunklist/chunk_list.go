package chunklist

import (
	"context"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Entry represents a single chunk within a chunk list.
type Entry struct {
	Offset uint64
	Digest digest.Digest
}

// ChunkList represents the ordered list of chunks that compose a blob.
type ChunkList []Entry

// Fetcher retrieves a ChunkList for a digest.
type Fetcher interface {
	FetchChunkList(ctx context.Context, digest digest.Digest) (ChunkList, error)
}

// FindChunkOffset returns the index of the chunk containing the given
// offset and the offset within that chunk.
func FindChunkOffset(chunkList ChunkList, off uint64) (index int, chunkOffset int64) {
	i, ok := slices.BinarySearchFunc(chunkList, off, func(e Entry, offset uint64) int {
		next := e.Offset + uint64(e.Digest.GetSizeBytes())
		if offset < e.Offset {
			return 1
		}
		if offset >= next {
			return -1
		}
		return 0
	})

	if ok {
		return i, int64(off - chunkList[i].Offset)
	}

	return len(chunkList), 0
}
