package chunk_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChunkEncodedRoundTrip(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		require.Equal(t, []byte("hello world"), c.GetBytes())
		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_IDENTITY)
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), encoded)
	})

	t.Run("ZSTD", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		decoded, err := zstd.Decode(encoded, len("hello world"))
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), decoded)
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		_, err := c.GetEncodedBytes(remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

func TestChunkFromEncoded(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		c, err := chunk.NewChunkFromEncoded([]byte("test data"), 100, remoteexecution.Compressor_IDENTITY)
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), c.GetBytes())
	})

	t.Run("IdentityBufferTooLarge", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("test data"), 4, remoteexecution.Compressor_IDENTITY)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("NegativeMaximumDecodedSize", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("test data"), -1, remoteexecution.Compressor_IDENTITY)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ZSTD", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		c, err := chunk.NewChunkFromEncoded(compressed, len([]byte("test data")), remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), c.GetBytes())

		// The chunk must hand back the exact bytes it was created
		// from, not a fresh encoding of the decoded data.
		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressed, encoded)
	})

	t.Run("ZSTDDecodeFailure", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("not valid zstd data"), 100, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("ZSTDBufferTooLarge", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		_, err := chunk.NewChunkFromEncoded(compressed, 4, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("test data"), 100, remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
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
