package chunk_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
)

func TestChunkUncompressedToCompressed(t *testing.T) {
	pool := zstd.NewUnboundedPool(nil, nil)

	t.Run("Success", func(t *testing.T) {
		c := chunk.NewChunk(pool, []byte("hello world"))

		require.Equal(t, []byte("hello world"), c.GetBytes())

		compressed, err := c.GetBytesCompressed(t.Context())
		require.NoError(t, err)

		decoder, err := pool.NewDecoder(t.Context(), bytes.NewReader(compressed))
		require.NoError(t, err)
		defer decoder.Close()

		decompressed, err := io.ReadAll(decoder)
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), decompressed)
	})
}

func TestChunkCompressedToUncompressed(t *testing.T) {
	pool := zstd.NewUnboundedPool(nil, nil)

	t.Run("Success", func(t *testing.T) {
		var buf bytes.Buffer
		enc, _ := pool.NewEncoder(t.Context(), &buf)
		enc.Write([]byte("test data"))
		enc.Close()

		c := chunk.NewChunkWithCompressedData([]byte("test data"), buf.Bytes())

		compressed, err := c.GetBytesCompressed(t.Context())
		require.NoError(t, err)
		require.Equal(t, buf.Bytes(), compressed)

		require.Equal(t, []byte("test data"), c.GetBytes())
	})
}

func TestEmptyChunk(t *testing.T) {
	pool := zstd.NewUnboundedPool(nil, nil)
	ctx := t.Context()

	require.Equal(t, []byte{}, chunk.EmptyChunk.GetBytes())

	compressed, err := chunk.EmptyChunk.GetBytesCompressed(ctx)
	require.NoError(t, err)

	decoder, err := pool.NewDecoder(t.Context(), bytes.NewReader(compressed))
	require.NoError(t, err)
	defer decoder.Close()

	decompressedData, err := io.ReadAll(decoder)
	require.NoError(t, err)
	require.Equal(t, []byte{}, decompressedData)
}
