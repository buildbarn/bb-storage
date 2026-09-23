package chunk

import (
	"context"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// List represents the ordered list of chunks that compose a blob.
type List struct {
	Offsets   []uint64
	Digests   []digest.Digest
	Validated bool
}

// ListFetcher retrieves a ChunkList for a digest.
type ListFetcher interface {
	FetchChunkList(ctx context.Context, digest digest.Digest) (List, error)
}

// FindChunkOffset returns the index of the chunk containing the given
// offset and the offset within that chunk.
func (l List) FindChunkOffset(off uint64) (index int, chunkOffset int64) {
	if len(l.Offsets) == 0 {
		return 0, 0
	}

	i, exact := slices.BinarySearch(l.Offsets, off)
	if exact {
		return i, 0
	}

	localOffset := int64(off - l.Offsets[i-1])
	if localOffset < l.Digests[i-1].GetSizeBytes() {
		return i - 1, localOffset
	}
	return len(l.Offsets), 0
}
