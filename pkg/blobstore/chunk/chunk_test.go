package chunk_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
)

func TestChunkCompressedRoundTrip(t *testing.T) {
	t.Run("Uncompressed", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		require.Equal(t, []byte("hello world"), c.GetBytes())
		decoded, err := zstd.Decode(c.GetBytesCompressed(), len("hello world"))
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), decoded)
	})

	t.Run("Compressed", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		c := chunk.NewChunkWithCompressedData([]byte("test data"), compressed)

		require.Equal(t, compressed, c.GetBytesCompressed())
		require.Equal(t, []byte("test data"), c.GetBytes())
	})

	t.Run("Precompressed", func(t *testing.T) {
		// Chunks created with compressed data pre-populated never
		// need to compress.
		c := chunk.NewChunkWithCompressedData([]byte("test data"), []byte("precomputed"))
		require.Equal(t, []byte("precomputed"), c.GetBytesCompressed())
	})
}
