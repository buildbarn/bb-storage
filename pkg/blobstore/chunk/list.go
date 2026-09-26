package chunk

import (
	"context"
	"math"
	"math/bits"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// List represents the ordered list of chunks that compose a blob.
//
// A chunk list always contain the digests of at least two chunks and no
// chunks of size zero. A chunk list may have already as being composed
// of the canonical chunks that concatenate to form the blob, in that
// case the Validated field should be true.
type List struct {
	Offsets   []uint64
	Digests   []digest.Digest
	Validated bool
}

// NewList constructs a chunk list composing a blob of sizeBytes bytes
// out of the provided chunks. This function will error for degenerate
// lists such as:
//
//   - Lists containing zero length chunks.
//   - Lists containing less than two chunks.
//   - Lists whose sizes do not add up to the expected size of
//     the blob.
func NewList(digests []digest.Digest, sizeBytes uint64, validated bool) (List, error) {
	offsets := make([]uint64, len(digests))
	offset := uint64(0)
	for i, d := range digests {
		if d.GetSizeBytes() == 0 {
			return List{}, status.Error(codes.Internal, "Chunk list contains a zero length chunk")
		}
		offsets[i] = offset
		var carry uint64
		offset, carry = bits.Add64(offset, uint64(d.GetSizeBytes()), 0)
		if carry != 0 {
			return List{}, status.Errorf(codes.Internal, "Chunk list overflows, the sum of chunk sizes exceeds %d bytes", uint64(math.MaxUint64))
		}
	}
	if offset != sizeBytes {
		return List{}, status.Error(codes.Internal, "Chunk list does not compose to blob")
	}
	if len(digests) < 2 {
		return List{}, status.Error(codes.Internal, "Chunk list contains fewer than two chunks")
	}
	return List{
		Offsets:   offsets,
		Digests:   digests,
		Validated: validated,
	}, nil
}

// ListFetcher retrieves a ChunkList for a digest.
type ListFetcher interface {
	FetchChunkList(ctx context.Context, digest digest.Digest) (List, error)
}

// FindChunkOffset returns the index of the chunk containing the given
// offset and the offset within that chunk. Offsets at or beyond the end
// of the blob yield an index equal to the number of chunks in the list.
func (l List) FindChunkOffset(off uint64) (index int, chunkOffset int64) {
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

// GetSizeBytes returns the cumulative size of the chunk list.
func (l List) GetSizeBytes() int64 {
	return int64(l.Offsets[len(l.Offsets)-1]) + l.Digests[len(l.Digests)-1].GetSizeBytes()
}
