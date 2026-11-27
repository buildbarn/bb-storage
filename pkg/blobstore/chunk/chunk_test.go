package chunk_test

import (
	"bytes"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChunkEncodedRoundTrip(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		require.Equal(t, []byte("hello world"), util.Must(c.GetBytes()))
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
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))
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
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))

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

func TestChunkWithDelayedValidation(t *testing.T) {
	data := bytes.Repeat([]byte("Hello"), 1000)
	md5 := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
	dataDigest := util.Must(chunk.NewChunk(data).ComputeDigest(md5))
	otherData := bytes.Repeat([]byte("Jello"), 1000)
	identity := remoteexecution.Compressor_IDENTITY
	zstdCompressor := remoteexecution.Compressor_ZSTD

	requireInvalid := func(t *testing.T, c *chunk.Chunk) {
		// All methods that provide access to the contents of
		// the chunk must fail.
		_, err := c.GetBytes()
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = c.GetEncodedBytes(identity)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = c.GetEncodedBytes(zstdCompressor)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = c.ComputeDigest(md5)
		require.Equal(t, codes.Internal, status.Code(err))
	}

	t.Run("IdentityValid", func(t *testing.T) {
		c, err := chunk.NewChunkWithDelayedValidation(data, dataDigest, identity)
		require.NoError(t, err)
		require.Equal(t, data, util.Must(c.GetUnvalidatedEncodedBytes(identity)))
		require.Equal(t, data, util.Must(c.GetBytes()))
		require.Equal(t, data, util.Must(c.GetEncodedBytes(identity)))
		require.Equal(t, dataDigest, util.Must(c.ComputeDigest(md5)))
		require.Equal(t, data, util.Must(zstd.Decode(util.Must(c.GetEncodedBytes(zstdCompressor)), len(data))))
	})

	t.Run("IdentityDigestMismatch", func(t *testing.T) {
		c, err := chunk.NewChunkWithDelayedValidation(otherData, dataDigest, identity)
		require.NoError(t, err)
		_, err = c.GetBytes()
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Digest mismatch, expected 3-5a8ff1bbbbc73d345c780cc11acee68e-5000-, got 3-4b17a5863f82ac00f4df1158ce01867e-5000-"), err)
		requireInvalid(t, c)

		// The chunk may still be forwarded to parties that
		// perform validation themselves.
		require.Equal(t, otherData, util.Must(c.GetUnvalidatedEncodedBytes(identity)))
		require.Equal(t, otherData, util.Must(zstd.Decode(util.Must(c.GetUnvalidatedEncodedBytes(zstdCompressor)), len(data))))
		requireInvalid(t, c)
	})

	t.Run("IdentitySizeMismatch", func(t *testing.T) {
		_, err := chunk.NewChunkWithDelayedValidation(data[:4999], dataDigest, identity)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Chunk is 4999 bytes in size, while 5000 bytes were expected"), err)
		_, err = chunk.NewChunkWithDelayedValidation(append(data, 'A'), dataDigest, identity)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ZSTDValid", func(t *testing.T) {
		compressed := zstd.Encode(data)
		c, err := chunk.NewChunkWithDelayedValidation(compressed, dataDigest, zstdCompressor)
		require.NoError(t, err)
		require.Equal(t, compressed, util.Must(c.GetUnvalidatedEncodedBytes(zstdCompressor)))
		require.Equal(t, data, util.Must(c.GetUnvalidatedEncodedBytes(identity)))
		require.Equal(t, data, util.Must(c.GetBytes()))
		require.Equal(t, compressed, util.Must(c.GetEncodedBytes(zstdCompressor)))
		require.Equal(t, dataDigest, util.Must(c.ComputeDigest(md5)))
	})

	t.Run("ZSTDDigestMismatch", func(t *testing.T) {
		// A well formed frame that holds different data of the
		// right size.
		compressed := zstd.Encode(otherData)
		c, err := chunk.NewChunkWithDelayedValidation(compressed, dataDigest, zstdCompressor)
		require.NoError(t, err)
		requireInvalid(t, c)

		require.Equal(t, compressed, util.Must(c.GetUnvalidatedEncodedBytes(zstdCompressor)))
		require.Equal(t, otherData, util.Must(c.GetUnvalidatedEncodedBytes(identity)))
		requireInvalid(t, c)
	})

	t.Run("ZSTDCorrupted", func(t *testing.T) {
		// A frame that cannot be decompressed can be forwarded
		// in compressed form, as that does not require looking
		// at it. Everything else fails.
		compressed := zstd.Encode(data)
		for i := 12; i < len(compressed); i++ {
			compressed[i] ^= 0xff
		}
		c, err := chunk.NewChunkWithDelayedValidation(compressed, dataDigest, zstdCompressor)
		require.NoError(t, err)
		require.Equal(t, compressed, util.Must(c.GetUnvalidatedEncodedBytes(zstdCompressor)))
		_, err = c.GetUnvalidatedEncodedBytes(identity)
		require.Equal(t, codes.Internal, status.Code(err))
		requireInvalid(t, c)
	})

	t.Run("ZSTDSizeMismatch", func(t *testing.T) {
		// Frames that decompress to too little or too much
		// data can't be decompressed, even without validation.
		for _, wrongData := range [][]byte{data[:4999], append(append([]byte(nil), data...), 'A')} {
			c, err := chunk.NewChunkWithDelayedValidation(zstd.Encode(wrongData), dataDigest, zstdCompressor)
			require.NoError(t, err)
			_, err = c.GetUnvalidatedEncodedBytes(identity)
			require.Equal(t, codes.Internal, status.Code(err))
			requireInvalid(t, c)
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		c, err := chunk.NewChunkWithDelayedValidation(zstd.Encode(data), dataDigest, zstdCompressor)
		require.NoError(t, err)

		// All callers must observe the very same slice.
		results := make([][]byte, 16)
		var wg sync.WaitGroup
		for i := range results {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if i%2 == 0 {
					results[i] = util.Must(c.GetBytes())
				} else {
					results[i] = util.Must(c.GetUnvalidatedEncodedBytes(identity))
				}
			}()
		}
		wg.Wait()
		for _, result := range results {
			require.Equal(t, data, result)
			require.Same(t, &results[0][0], &result[0])
		}
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		_, err := chunk.NewChunkWithDelayedValidation(data, dataDigest, remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))

		c, err := chunk.NewChunkWithDelayedValidation(data, dataDigest, identity)
		require.NoError(t, err)
		_, err = c.GetUnvalidatedEncodedBytes(remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("AlreadyValidatedChunk", func(t *testing.T) {
		// For regular chunks there is no difference.
		c := chunk.NewChunk(data)
		require.Equal(t, data, util.Must(c.GetUnvalidatedEncodedBytes(identity)))
		require.Equal(t, util.Must(c.GetEncodedBytes(zstdCompressor)), util.Must(c.GetUnvalidatedEncodedBytes(zstdCompressor)))
	})
}

func TestChunkComputeDigest(t *testing.T) {
	c := chunk.NewChunk([]byte("Hello"))

	require.Equal(
		t,
		digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		util.Must(c.ComputeDigest(digest.MustNewFunction("", remoteexecution.DigestFunction_MD5))),
	)
}
