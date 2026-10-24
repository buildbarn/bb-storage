package chunk_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

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
