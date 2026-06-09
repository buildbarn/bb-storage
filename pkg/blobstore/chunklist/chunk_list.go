package chunklist

import (
	"context"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// ChunkList represents the ordered list of chunks that compose a blob.
type ChunkList struct {
	Offsets   []uint64
	Digests   []digest.Digest
	Validated bool
}

// Fetcher retrieves a ChunkList for a digest.
type Fetcher interface {
	FetchChunkList(ctx context.Context, digest digest.Digest) (ChunkList, error)
}

// FindChunkOffset returns the index of the chunk containing the given
// offset and the offset within that chunk.
func FindChunkOffset(chunkList ChunkList, off uint64) (index int, chunkOffset int64) {
	if len(chunkList.Offsets) == 0 {
		return 0, 0
	}

	i, exact := slices.BinarySearch(chunkList.Offsets, off)
	if exact {
		return i, 0
	}

	localOffset := int64(off - chunkList.Offsets[i-1])
	if localOffset < chunkList.Digests[i-1].GetSizeBytes() {
		return i - 1, localOffset
	}
	return len(chunkList.Offsets), 0
}
