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

// Mapping represents the ordered mapping of chunks that compose a blob.
//
// A chunk mapping always contain the digests of at least two chunks and no
// chunks of size zero. A chunk mapping may have already as being composed
// of the canonical chunks that concatenate to form the blob, in that
// case the Validated field should be true.
type Mapping struct {
	Offsets   []uint64
	Digests   []digest.Digest
	Validated bool
}

// NewMapping constructs a chunk mapping composing a blob of sizeBytes bytes
// out of the provided chunks. This function will error for degenerate
// mappings such as:
//
//   - Mappings containing zero length chunks.
//   - Mappings containing less than two chunks.
//   - Mappings whose sizes do not add up to the expected size of
//     the blob.
func NewMapping(digests []digest.Digest, sizeBytes uint64, validated bool) (Mapping, error) {
	offsets := make([]uint64, len(digests))
	offset := uint64(0)
	for i, d := range digests {
		if d.GetSizeBytes() == 0 {
			return Mapping{}, status.Error(codes.Internal, "Chunk mapping contains a zero length chunk")
		}
		offsets[i] = offset
		var carry uint64
		offset, carry = bits.Add64(offset, uint64(d.GetSizeBytes()), 0)
		if carry != 0 {
			return Mapping{}, status.Errorf(codes.Internal, "Chunk mapping overflows, the sum of chunk sizes exceeds %d bytes", uint64(math.MaxUint64))
		}
	}
	if offset != sizeBytes {
		return Mapping{}, status.Error(codes.Internal, "Chunk mapping does not compose to blob")
	}
	if len(digests) < 2 {
		return Mapping{}, status.Error(codes.Internal, "Chunk mapping contains fewer than two chunks")
	}
	return Mapping{
		Offsets:   offsets,
		Digests:   digests,
		Validated: validated,
	}, nil
}

// MappingFetcher retrieves a ChunkMapping for a digest.
type MappingFetcher interface {
	FetchChunkMapping(ctx context.Context, digest digest.Digest) (Mapping, error)
}

// FindChunkOffset returns the index of the chunk containing the given
// offset and the offset within that chunk. Offsets at or beyond the end
// of the blob yield an index equal to the number of chunks in the mapping.
func (l Mapping) FindChunkOffset(off uint64) (index int, chunkOffset int64) {
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

// GetSizeBytes returns the cumulative size of the chunk mapping.
func (l Mapping) GetSizeBytes() int64 {
	return int64(l.Offsets[len(l.Offsets)-1]) + l.Digests[len(l.Digests)-1].GetSizeBytes()
}
