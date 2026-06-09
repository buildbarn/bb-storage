package buffer_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChunkUncompressedToCompressed(t *testing.T) {
	pool := zstd.NewUnboundedPool(nil, nil)

	t.Run("Success", func(t *testing.T) {
		c := buffer.NewChunk(pool, []byte("hello world"))

		data, err := c.GetBytes(t.Context())
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), data)

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

		c := buffer.NewChunkFromCompressedData(pool, buf.Bytes())

		compressed, err := c.GetBytesCompressed(t.Context())
		require.NoError(t, err)
		require.Equal(t, buf.Bytes(), compressed)

		data, err := c.GetBytes(t.Context())
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), data)
	})

	t.Run("Failure", func(t *testing.T) {
		c := buffer.NewChunkFromCompressedData(pool, []byte("This is not valid zstd data"))

		data, err := c.GetBytesCompressed(t.Context())
		require.NoError(t, err)
		require.Equal(t, []byte("This is not valid zstd data"), data)

		_, err = c.GetBytes(t.Context())
		testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Could not decompress data: "), err)
	})
}

func TestEmptyChunk(t *testing.T) {
	pool := zstd.NewUnboundedPool(nil, nil)
	ctx := t.Context()

	data, err := buffer.EmptyChunk.GetBytes(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte{}, data)

	compressed, err := buffer.EmptyChunk.GetBytesCompressed(ctx)
	require.NoError(t, err)

	decoder, err := pool.NewDecoder(t.Context(), bytes.NewReader(compressed))
	require.NoError(t, err)
	defer decoder.Close()

	decompressedData, err := io.ReadAll(decoder)
	require.NoError(t, err)
	require.Equal(t, []byte{}, decompressedData)
}
