package chunk

import (
	"encoding/hex"
	"math"
	"math/bits"
	"sort"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
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
	digestFunction digest.Function
	digests        []byte
	offsets        []uint64
	size           uint64
	validated      bool
}

// NewMappingFromDigests constructs a chunk mapping composing a blob of
// sizeBytes bytes out of the provided chunks. This function will error
// for degenerate mappings such as:
//
//   - Mappings containing zero length chunks.
//   - Mappings containing less than two chunks.
//   - Mappings whose sizes do not add up to the expected size of
//     the blob.
//   - Mappings containing digests with different digest functions.
func NewMappingFromDigests(digests []digest.Digest, sizeBytes uint64, validated bool) (Mapping, error) {
	offsets := make([]uint64, len(digests))
	offset := uint64(0)
	for i, d := range digests {
		if d.GetSizeBytes() == 0 {
			return Mapping{}, status.Error(codes.InvalidArgument, "Chunk mapping contains a zero length chunk")
		}
		offsets[i] = offset
		var carry uint64
		offset, carry = bits.Add64(offset, uint64(d.GetSizeBytes()), 0)
		if carry != 0 {
			return Mapping{}, status.Errorf(codes.InvalidArgument, "Chunk mapping overflows, the sum of chunk sizes exceeds %d bytes", uint64(math.MaxUint64))
		}
	}
	if offset != sizeBytes {
		return Mapping{}, status.Error(codes.InvalidArgument, "Chunk mapping does not compose to blob")
	}
	if len(digests) < 2 {
		return Mapping{}, status.Error(codes.InvalidArgument, "Chunk mapping contains fewer than two chunks")
	}
	digestFunction := digests[0].GetDigestFunction()
	hashLen := len(digests[0].GetHashBytes())
	digestBytes := make([]byte, 0, len(digests)*hashLen)
	for _, d := range digests {
		hashBytes := d.GetHashBytes()
		if len(hashBytes) != hashLen {
			return Mapping{}, status.Error(codes.InvalidArgument, "Chunk mapping contains digests with different digest functions")
		}
		digestBytes = append(digestBytes, hashBytes...)
	}
	return Mapping{
		digestFunction: digestFunction,
		digests:        digestBytes,
		offsets:        offsets,
		size:           sizeBytes,
		validated:      validated,
	}, nil
}

// NewMappingFromProtoDigests constructs a chunk mapping composing a blob
// of sizeBytes bytes out of the provided protocol-level digests, using
// the given digest function to interpret them. See NewMappingFromDigests
// for the cases in which this function errors.
func NewMappingFromProtoDigests(digestFunction digest.Function, protoDigests []*remoteexecution.Digest, sizeBytes uint64, validated bool) (Mapping, error) {
	// TODO: This currently constructs an intermediate digest.Digest per
	// chunk, which allocates a full digest string and hex encodes/decodes
	// the hash right after one another. We could instead hex decode the
	// hashes straight into the packed digests of the mapping, at the cost
	// of duplicating part of the validation performed by digest.Function.
	digests := make([]digest.Digest, 0, len(protoDigests))
	for _, protoDigest := range protoDigests {
		d, err := digestFunction.NewDigestFromProto(protoDigest)
		if err != nil {
			return Mapping{}, err
		}
		digests = append(digests, d)
	}
	return NewMappingFromDigests(digests, sizeBytes, validated)
}

// Length returns the number of chunks in the mapping.
func (m Mapping) Length() int {
	return len(m.offsets)
}

// GetDigestAtIndex returns the digest of the chunk at the given index.
// The index MUST be smaller than Length().
func (m Mapping) GetDigestAtIndex(i int) digest.Digest {
	hashLen := len(m.digests) / len(m.offsets)
	d, err := m.digestFunction.NewDigest(
		hex.EncodeToString(m.digests[i*hashLen:(i+1)*hashLen]),
		int64(m.getChunkSize(i)),
	)
	if err != nil {
		panic("unreachable: digests of a chunk mapping are always valid")
	}
	return d
}

// GetDigests returns the digests of all chunks in the mapping.
func (m Mapping) GetDigests() []digest.Digest {
	digests := make([]digest.Digest, len(m.offsets))
	for i := range m.offsets {
		digests[i] = m.GetDigestAtIndex(i)
	}
	return digests
}

// GetProtoDigests returns the digests of all chunks in the mapping in
// protocol-level format. Callers should only call this method if they
// actually need the full array, as constructing it requires an
// allocation and parsing of the packed digests.
func (m Mapping) GetProtoDigests() []*remoteexecution.Digest {
	protoDigests := make([]*remoteexecution.Digest, len(m.offsets))
	for i := range m.offsets {
		protoDigests[i] = m.GetDigestAtIndex(i).GetProto()
	}
	return protoDigests
}

// IsValidated returns whether the chunk mapping has already been
// determined to compose the digest which it is being addressed by.
func (m Mapping) IsValidated() bool {
	return m.validated
}

// FindChunkOffset returns the index of the chunk containing the given
// offset and the offset within that chunk. The requested offset MUST
// not be at or beyond the end of the mapping.
func (m Mapping) FindChunkOffset(off uint64) (index int, chunkOffset int64) {
	i := sort.Search(len(m.offsets), func(i int) bool {
		return m.offsets[i] > off
	}) - 1
	return i, int64(off - m.offsets[i])
}

// getChunkSize returns the size of the chunk at the given index.
func (m Mapping) getChunkSize(i int) uint64 {
	if i+1 < len(m.offsets) {
		return m.offsets[i+1] - m.offsets[i]
	}
	return m.size - m.offsets[i]
}

// GetSizeBytes returns the cumulative size of the chunk mapping.
func (m Mapping) GetSizeBytes() int64 {
	return int64(m.size)
}
