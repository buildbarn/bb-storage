package chunk_test

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestMappingEquals verifies that Equals() only considers the chunks
// that make up the mapping, not how the mapping was constructed.
func TestMappingEquals(t *testing.T) {
	md5 := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
	sha256 := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)

	digestA, err := md5.NewDigest("00000000000000000000000000000000", 3)
	require.NoError(t, err)
	digestB, err := md5.NewDigest("11111111111111111111111111111111", 4)
	require.NoError(t, err)
	digestC, err := md5.NewDigest("22222222222222222222222222222222", 5)
	require.NoError(t, err)
	digestASha256, err := sha256.NewDigest("0000000000000000000000000000000000000000000000000000000000000000", 3)
	require.NoError(t, err)
	digestBSha256, err := sha256.NewDigest("1111111111111111111111111111111111111111111111111111111111111111", 4)
	require.NoError(t, err)

	mapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestB}, 7, false)
	require.NoError(t, err)
	validatedMapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestB}, 7, true)
	require.NoError(t, err)
	reorderedMapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestB, digestA}, 7, false)
	require.NoError(t, err)
	resizedMapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestC}, 8, false)
	require.NoError(t, err)
	otherFunctionMapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestASha256, digestBSha256}, 7, false)
	require.NoError(t, err)

	// The validated flag does not affect equality.
	require.True(t, mapping.Equals(validatedMapping))
	require.True(t, validatedMapping.Equals(mapping))

	// Chunk order, chunk sizes and digest function do.
	require.False(t, mapping.Equals(reorderedMapping))
	require.False(t, mapping.Equals(resizedMapping))
	require.False(t, mapping.Equals(otherFunctionMapping))
}

// TestMappingCompactBinaryRoundTrip verifies that ToCompactBinary()
// and NewMappingFromCompactBinary() are each other's inverse, and that
// the compact binary representation has the documented layout.
func TestMappingCompactBinaryRoundTrip(t *testing.T) {
	for _, digestFunction := range []digest.Function{
		digest.MustNewFunction("", remoteexecution.DigestFunction_MD5),
		digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
	} {
		hashLength := digestFunction.GetHashLength()
		digestA, err := digestFunction.NewDigest(strings.Repeat("01", hashLength), 3)
		require.NoError(t, err)
		digestB, err := digestFunction.NewDigest(strings.Repeat("02", hashLength), 4)
		require.NoError(t, err)

		mapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestB}, 7, true)
		require.NoError(t, err)

		compactBinary, err := mapping.ToCompactBinary()
		require.NoError(t, err)
		expected := []byte{3, 0, 0, 0, 4, 0, 0, 0}
		expected = append(expected, digestA.GetHashBytes()...)
		expected = append(expected, digestB.GetHashBytes()...)
		require.Equal(t, expected, compactBinary)

		decoded, err := chunk.NewMappingFromCompactBinary(compactBinary, digestFunction, true)
		require.NoError(t, err)
		require.True(t, mapping.Equals(decoded))
		require.True(t, decoded.IsValidated())

		decodedUnvalidated, err := chunk.NewMappingFromCompactBinary(compactBinary, digestFunction, false)
		require.NoError(t, err)
		require.True(t, mapping.Equals(decodedUnvalidated))
		require.False(t, decodedUnvalidated.IsValidated())

		// The decoded mapping must not alias the input data.
		for i := range compactBinary {
			compactBinary[i] = 0
		}
		require.True(t, mapping.Equals(decoded))
	}
}

// TestMappingCompactBinaryLargeBlob verifies that chunks of up to
// MaxUint32 bytes are representable, and that blobs larger than
// MaxUint32 bytes round trip correctly as long as every individual
// chunk is at most MaxUint32 bytes.
func TestMappingCompactBinaryLargeBlob(t *testing.T) {
	md5 := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
	chunkSize := int64(math.MaxUint32)
	digestA, err := md5.NewDigest(strings.Repeat("01", md5.GetHashLength()), chunkSize)
	require.NoError(t, err)
	digestB, err := md5.NewDigest(strings.Repeat("02", md5.GetHashLength()), chunkSize)
	require.NoError(t, err)

	mapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestB}, 2*uint64(chunkSize), false)
	require.NoError(t, err)

	compactBinary, err := mapping.ToCompactBinary()
	require.NoError(t, err)

	decoded, err := chunk.NewMappingFromCompactBinary(compactBinary, md5, false)
	require.NoError(t, err)
	require.True(t, mapping.Equals(decoded))
}

// TestMappingCompactBinaryInvalidData verifies that
// NewMappingFromCompactBinary() rejects malformed data.
func TestMappingCompactBinaryInvalidData(t *testing.T) {
	md5 := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
	sha256 := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
	entrySize := 4 + md5.GetHashLength()

	// Data must consist of a whole number of chunks.
	_, err := chunk.NewMappingFromCompactBinary(make([]byte, entrySize+1), md5, false)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Data does not add up to a whole number of chunks"), err)

	// Data must contain at least two chunks.
	_, err = chunk.NewMappingFromCompactBinary(make([]byte, entrySize), md5, false)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Chunk mapping contains fewer than two chunks"), err)

	// Chunks may not be of zero length.
	data := make([]byte, 2*entrySize)
	binary.LittleEndian.PutUint32(data, 1)
	_, err = chunk.NewMappingFromCompactBinary(data, md5, false)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Chunk mapping contains a zero length chunk"), err)

	// The digest function must match the data.
	_, err = chunk.NewMappingFromCompactBinary(make([]byte, 2*entrySize), sha256, false)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Data does not add up to a whole number of chunks"), err)
}

// TestMappingToCompactBinaryRejectsLargeChunk verifies that chunk
// mappings containing chunks larger than MaxUint32 bytes can be
// constructed, but cannot be serialized.
func TestMappingToCompactBinaryRejectsLargeChunk(t *testing.T) {
	md5 := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
	digestA, err := md5.NewDigest(strings.Repeat("01", md5.GetHashLength()), 1)
	require.NoError(t, err)
	digestB, err := md5.NewDigest(strings.Repeat("02", md5.GetHashLength()), math.MaxUint32+1)
	require.NoError(t, err)

	mapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestB}, math.MaxUint32+2, false)
	require.NoError(t, err)

	_, err = mapping.ToCompactBinary()
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Chunk at index 1 has size 4294967296, while the compact binary representation only supports chunks of up to 4294967295 bytes"), err)
}

// FuzzMappingFindChunkOffset verifies that FindChunkOffset() assigns
// every offset within a chunk mapping to the chunk that contains it.
//
// Both the layout of the chunk mapping and the offset that is looked up
// are fuzzed. The result of the binary search performed by
// FindChunkOffset() is compared against a linear scan over the chunk
// sizes, which is trivially correct.
func FuzzMappingFindChunkOffset(f *testing.F) {
	f.Add([]byte{1, 2, 3}, uint64(0))
	f.Add([]byte{1, 1}, uint64(1))
	f.Add([]byte{255, 255}, uint64(509))
	f.Add([]byte{0, 1, 2}, uint64(1))

	f.Fuzz(func(t *testing.T, sizes []byte, off uint64) {
		// Zero length chunks are rejected by
		// NewMappingFromDigests(), so discard them from the input.
		chunkSizes := make([]byte, 0, len(sizes))
		for _, size := range sizes {
			if size != 0 {
				chunkSizes = append(chunkSizes, size)
			}
		}
		if len(chunkSizes) < 2 {
			return
		}

		digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
		digests := make([]digest.Digest, 0, len(chunkSizes))
		var sizeBytes uint64
		for _, chunkSize := range chunkSizes {
			d, err := digestFunction.NewDigest(
				"00000000000000000000000000000000",
				int64(chunkSize),
			)
			require.NoError(t, err)
			digests = append(digests, d)
			sizeBytes += uint64(chunkSize)
		}

		mapping, err := chunk.NewMappingFromDigests(digests, sizeBytes, false)
		require.NoError(t, err)
		require.Equal(t, len(chunkSizes), mapping.Length())

		// FindChunkOffset() is only defined for offsets that lie
		// within the blob, so fold the fuzzed offset into that range.
		off %= sizeBytes

		// Determine the expected result by scanning over the chunk
		// sizes one by one.
		expectedIndex := 0
		expectedChunkOffset := off
		for expectedChunkOffset >= uint64(chunkSizes[expectedIndex]) {
			expectedChunkOffset -= uint64(chunkSizes[expectedIndex])
			expectedIndex++
		}

		index, chunkOffset := mapping.FindChunkOffset(off)
		require.Equal(t, expectedIndex, index, "Offset %d was assigned to the wrong chunk", off)
		require.Equal(t, int64(expectedChunkOffset), chunkOffset, "Offset %d was assigned the wrong offset within its chunk", off)
	})
}
