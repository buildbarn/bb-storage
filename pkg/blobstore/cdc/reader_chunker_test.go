package cdc_test

import (
	"io"
	"math/rand"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

const (
	minChunkSize          = 256 << 10 // 256 KiB
	maxChunkSize          = 2*minChunkSize - 1
	horizonLookaheadBytes = 8 * minChunkSize
)

func FuzzReaderChunker(f *testing.F) {
	for i := range 20 {
		// Fuzz test i+1 MB of data with seed i.
		f.Add((i+1)<<20, int64(i))
	}
	f.Fuzz(func(t *testing.T, dataSizeBytes int, seed int64) {
		require := require.New(t)
		rng := rand.New(rand.NewSource(seed))
		originalData := make([]byte, dataSizeBytes)
		rng.Read(originalData)
		digestFunc := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)

		reader := buffer.NewValidatedBufferFromByteSlice(originalData).ToReader()
		defer reader.Close()
		chunker := cdc.NewReaderChunker(digestFunc, reader, minChunkSize, horizonLookaheadBytes)

		composedData := make([]byte, 0, dataSizeBytes)
		var numberOfChunks int
		for numberOfChunks = 0; ; numberOfChunks++ {
			chunk, err := chunker.NextChunk()
			if err == io.EOF {
				break
			}
			require.NoError(err, "Failed to generate chunk %d.", numberOfChunks)

			chunkSize := int64(len(chunk.Data))
			chunkHasher := chunk.Digest.NewHasher(chunkSize)
			chunkHasher.Write(chunk.Data)

			require.Equal(chunk.Digest.GetHashBytes(), chunkHasher.Sum(nil), "Digest mismatch for %d.", numberOfChunks)
			composedData = append(composedData, chunk.Data...)
		}

		require.Equal(originalData, composedData)

		originalDigestGen := digestFunc.NewGenerator(int64(dataSizeBytes))
		originalDigestGen.Write(originalData)

		composedDigestGen := digestFunc.NewGenerator(int64(dataSizeBytes))
		composedDigestGen.Write(composedData)

		require.Equal(originalDigestGen.Sum(), composedDigestGen.Sum(), "The digest of the composed data does not match the digest of the original data.")

		minNumberOfChunks := dataSizeBytes / maxChunkSize
		require.GreaterOrEqual(numberOfChunks, minNumberOfChunks, "Produced fewer chunks than should be possible.")

		maxNumberOfChunks := dataSizeBytes / minChunkSize
		require.LessOrEqual(numberOfChunks, maxNumberOfChunks, "Produced more chunks than should be possible.")
	})
}

func TestReaderChunkerSmallBlob(t *testing.T) {
	// Test with a small blob that should produce a single chunk
	originalData := []byte("Hello, World!")
	reader := buffer.NewValidatedBufferFromByteSlice(originalData).ToReader()
	defer reader.Close()

	digestFunc := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
	chunker := cdc.NewReaderChunker(digestFunc, reader, minChunkSize, horizonLookaheadBytes)

	chunks := make([][]byte, 0, 1)
	for {
		chunk, err := chunker.NextChunk()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		chunks = append(chunks, chunk.Data)
	}
	require.Len(t, chunks, 1)
	require.Equal(t, originalData, chunks[0])
}

func TestReaderChunkerEmptyBlob(t *testing.T) {
	// Test with empty blob
	originalData := []byte{}
	reader := buffer.NewValidatedBufferFromByteSlice(originalData).ToReader()
	defer reader.Close()

	digestFunc := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
	chunker := cdc.NewReaderChunker(digestFunc, reader, minChunkSize, horizonLookaheadBytes)

	chunk, err := chunker.NextChunk()
	require.ErrorIs(t, io.EOF, err)
	require.Empty(t, chunk.Data)
}
