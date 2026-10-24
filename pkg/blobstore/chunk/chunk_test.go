package chunk_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
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

	t.Run("CompressedSuccess", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		c, err := chunk.NewChunkCompressed(compressed, len([]byte("test data")))
		require.NoError(t, err)
		require.Equal(t, compressed, c.GetBytesCompressed())
		require.Equal(t, []byte("test data"), c.GetBytes())
	})

	t.Run("CompressedFailure", func(t *testing.T) {
		_, err := chunk.NewChunkCompressed([]byte("not valid zstd data"), 100)
		require.Error(t, err)
	})

	t.Run("CompressedBufferTooLarge", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		_, err := chunk.NewChunkCompressed(compressed, 4)
		require.Error(t, err)
	})
}

func TestChunkComputeDigest(t *testing.T) {
	c := chunk.NewChunk([]byte("Hello"))

	require.Equal(
		t,
		digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		c.ComputeDigest(digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)),
	)
}
