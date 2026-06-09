package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewDigestFromByteStreamReadPath(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"), err)
	})

	t.Run("BlabsInsteadOfBlobs", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("blabs/8b1a9953c4611296a827abf8c47804d7/123")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"), err)
	})

	t.Run("NonIntegerSize", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("blobs/8b1a9953c4611296a827abf8c47804d7/five")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid blob size \"five\""), err)
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("x/operations/y/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid instance name \"x/operations/y\": Instance name contains reserved keyword \"operations\""), err)
	})

	t.Run("UnknownCompressionMethod", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("x/compressed-blobs/xyzzy/8b1a9953c4611296a827abf8c47804d7/123")
		testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "Unsupported compression scheme \"xyzzy\""), err)
	})

	t.Run("NoInstanceName", func(t *testing.T) {
		t.Run("BLAKE3", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamReadPath("blobs/blake3/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_BLAKE3, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})

		t.Run("GITSHA1", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamReadPath("blobs/gitsha1/e22e8f5c4057251e65ab28c75ef3f7c2c2e7fe32/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_GITSHA1, "e22e8f5c4057251e65ab28c75ef3f7c2c2e7fe32", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})

		t.Run("MD5", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamReadPath("blobs/8b1a9953c4611296a827abf8c47804d7/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})

		t.Run("SHA256TREE", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamReadPath("blobs/sha256tree/0f7b3dc589fa10959e9507ad24e7e1197dd56f2ebbc006d4c9a2a3074a72fc8c/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256TREE, "0f7b3dc589fa10959e9507ad24e7e1197dd56f2ebbc006d4c9a2a3074a72fc8c", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/world/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("RedundantSlashes", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("//hello//world//blobs//8b1a9953c4611296a827abf8c47804d7//123//")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("Zstandard", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/world/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_ZSTD, compressor)
	})

	t.Run("Deflate", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/world/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_DEFLATE, compressor)
	})
}

func TestNewDigestFromByteStreamWritePath(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"), err)
	})

	t.Run("DownloadsInsteadOfUploads", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("downloads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"), err)
	})

	t.Run("NonIntegerSize", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/five")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid blob size \"five\""), err)
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("x/operations/y/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid instance name \"x/operations/y\": Instance name contains reserved keyword \"operations\""), err)
	})

	t.Run("UnknownCompressionMethod", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("x/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/compressed-blobs/xyzzy/8b1a9953c4611296a827abf8c47804d7/123")
		testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "Unsupported compression scheme \"xyzzy\""), err)
	})

	t.Run("NoInstanceName", func(t *testing.T) {
		t.Run("BLAKE3", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamWritePath("uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/blake3/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_BLAKE3, "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})

		t.Run("GITSHA1", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamWritePath("uploads/f50c65cd-9bbe-467d-8b9e-86c2b98c0d6a/blobs/gitsha1/f360a89d2669a6de05a290240553e64f100a4741/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_GITSHA1, "f360a89d2669a6de05a290240553e64f100a4741", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})

		t.Run("MD5", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamWritePath("uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})

		t.Run("SHA256TREE", func(t *testing.T) {
			d, compressor, err := digest.NewDigestFromByteStreamWritePath("uploads/8ede80b5-b598-4ada-be7e-c673479773c3/blobs/sha256tree/d713668f0f0c955bc5eef4432185ebb9d84d340695b4efa3645093fa1802a87c/123")
			require.NoError(t, err)
			require.Equal(t, digest.MustNewDigest("", remoteexecution.DigestFunction_SHA256TREE, "d713668f0f0c955bc5eef4432185ebb9d84d340695b4efa3645093fa1802a87c", 123), d)
			require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
		})
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("RedundantSlashes", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("//hello//world//uploads//da2f1135-326b-4956-b920-1646cdd6cb53//blobs//8b1a9953c4611296a827abf8c47804d7//123//")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("TrailingPath", func(t *testing.T) {
		// Upload paths may contain a trailing filename that the
		// implementation can use to attach a name to the
		// object. This implementation ignores that information.
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123/this/file/is/called/foo.txt")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("Zstandard", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_ZSTD, compressor)
	})

	t.Run("Deflate", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_DEFLATE, compressor)
	})
}

func TestDigestGetByteStreamReadPath(t *testing.T) {
	t.Run("NoInstanceName", func(t *testing.T) {
		t.Run("BLAKE3", func(t *testing.T) {
			require.Equal(
				t,
				"blobs/blake3/af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_BLAKE3,
					"af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262",
					123,
				).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
			)
		})

		t.Run("GITSHA1", func(t *testing.T) {
			require.Equal(
				t,
				"blobs/gitsha1/56a69bf74dc325e10e19ab2c69c13d1360aea147/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_GITSHA1,
					"56a69bf74dc325e10e19ab2c69c13d1360aea147",
					123,
				).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
			)
		})

		t.Run("MD5", func(t *testing.T) {
			require.Equal(
				t,
				"blobs/8b1a9953c4611296a827abf8c47804d7/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_MD5,
					"8b1a9953c4611296a827abf8c47804d7",
					123,
				).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
			)
		})

		t.Run("SHA256TREE", func(t *testing.T) {
			require.Equal(
				t,
				"blobs/sha256tree/23cba29b38d57014880a2963abda1c7e32b567ab83c64b998adbd3928c5f2e40/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_SHA256TREE,
					"23cba29b38d57014880a2963abda1c7e32b567ab83c64b998adbd3928c5f2e40",
					123,
				).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
			)
		})
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		require.Equal(
			t,
			"hello/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_MD5,
				"8b1a9953c4611296a827abf8c47804d7",
				123,
			).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
		)
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d := digest.MustNewDigest(
			"hello/world",
			remoteexecution.DigestFunction_MD5,
			"8b1a9953c4611296a827abf8c47804d7",
			123,
		)

		require.Equal(
			t,
			"hello/world/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY),
		)
		require.Equal(
			t,
			"hello/world/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD),
		)
		require.Equal(
			t,
			"hello/world/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamReadPath(remoteexecution.Compressor_DEFLATE),
		)
	})
}
