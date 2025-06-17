package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
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
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid blob size \"forty-two\""), err)
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
		t.Run("MD5", func(t *testing.T) {
			require.Equal(
				t,
				"blobs/8b1a9953c4611296a827abf8c47804d7/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_MD5,
					"8b1a9953c4611296a827abf8c47804d7",
					123).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
		})

		t.Run("SHA256TREE", func(t *testing.T) {
			require.Equal(
				t,
				"blobs/sha256tree/23cba29b38d57014880a2963abda1c7e32b567ab83c64b998adbd3928c5f2e40/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_SHA256TREE,
					"23cba29b38d57014880a2963abda1c7e32b567ab83c64b998adbd3928c5f2e40",
					123).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
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
				123).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d := digest.MustNewDigest(
			"hello/world",
			remoteexecution.DigestFunction_MD5,
			"8b1a9953c4611296a827abf8c47804d7",
			123)

		require.Equal(
			t,
			"hello/world/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
		require.Equal(
			t,
			"hello/world/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamReadPath(remoteexecution.Compressor_ZSTD))
		require.Equal(
			t,
			"hello/world/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamReadPath(remoteexecution.Compressor_DEFLATE))
	})
}

func TestDigestGetByteStreamWritePath(t *testing.T) {
	uuid := uuid.Must(uuid.Parse("36ebab65-3c4f-4faf-818b-2eabb4cd1b02"))

	t.Run("NoInstanceName", func(t *testing.T) {
		t.Run("MD5", func(t *testing.T) {
			require.Equal(
				t,
				"uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/blobs/8b1a9953c4611296a827abf8c47804d7/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_MD5,
					"8b1a9953c4611296a827abf8c47804d7",
					123).GetByteStreamWritePath(uuid, remoteexecution.Compressor_IDENTITY))
		})

		t.Run("SHA256TREE", func(t *testing.T) {
			require.Equal(
				t,
				"uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/blobs/sha256tree/e58ef976160845c07f7be8dedf6f36194acb958f84cd2bbff74161e07ba5fcca/123",
				digest.MustNewDigest(
					"",
					remoteexecution.DigestFunction_SHA256TREE,
					"e58ef976160845c07f7be8dedf6f36194acb958f84cd2bbff74161e07ba5fcca",
					123).GetByteStreamWritePath(uuid, remoteexecution.Compressor_IDENTITY))
		})
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		require.Equal(
			t,
			"hello/uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_MD5,
				"8b1a9953c4611296a827abf8c47804d7",
				123).GetByteStreamWritePath(uuid, remoteexecution.Compressor_IDENTITY))
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d := digest.MustNewDigest(
			"hello/world",
			remoteexecution.DigestFunction_MD5,
			"8b1a9953c4611296a827abf8c47804d7",
			123)

		require.Equal(
			t,
			"hello/world/uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamWritePath(uuid, remoteexecution.Compressor_IDENTITY))
		require.Equal(
			t,
			"hello/world/uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamWritePath(uuid, remoteexecution.Compressor_ZSTD))
		require.Equal(
			t,
			"hello/world/uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123",
			d.GetByteStreamWritePath(uuid, remoteexecution.Compressor_DEFLATE))
	})
}

func TestDigestGetProto(t *testing.T) {
	t.Run("SHA256", func(t *testing.T) {
		require.Equal(
			t,
			&remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 123,
			},
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_SHA256,
				"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				123).GetProto())
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		require.Equal(
			t,
			&remoteexecution.Digest{
				Hash:      "53f1472ebcd5c796407a98de5714a5fd39cb354dfe67a187a5b026fedd610e60",
				SizeBytes: 123,
			},
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_SHA256TREE,
				"53f1472ebcd5c796407a98de5714a5fd39cb354dfe67a187a5b026fedd610e60",
				123).GetProto())
	})
}

func TestDigestGetInstanceName(t *testing.T) {
	require.Equal(
		t,
		util.Must(digest.NewInstanceName("hello")),
		digest.MustNewDigest(
			"hello",
			remoteexecution.DigestFunction_SHA256,
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetInstanceName())
}

func TestDigestGetHashBytes(t *testing.T) {
	t.Run("SHA256", func(t *testing.T) {
		require.Equal(
			t,
			[]byte{
				0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14,
				0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f, 0xb9, 0x24,
				0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c,
				0xa4, 0x95, 0x99, 0x1b, 0x78, 0x52, 0xb8, 0x55,
			},
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_SHA256,
				"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				123).GetHashBytes())
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		require.Equal(
			t,
			[]byte{
				0xa4, 0xed, 0x98, 0x9b, 0xce, 0x5e, 0x10, 0xaf,
				0xae, 0x02, 0xd1, 0xb4, 0xe2, 0xa4, 0xfb, 0xf4,
				0x35, 0xcc, 0x14, 0x85, 0x09, 0xed, 0x4d, 0xb7,
				0xdc, 0x35, 0x45, 0x48, 0x83, 0x0c, 0x45, 0xa7,
			},
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_SHA256TREE,
				"a4ed989bce5e10afae02d1b4e2a4fbf435cc148509ed4db7dc354548830c45a7",
				123).GetHashBytes())
	})
}

func TestDigestGetHashString(t *testing.T) {
	t.Run("SHA256", func(t *testing.T) {
		require.Equal(
			t,
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_SHA256,
				"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				123).GetHashString())
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		require.Equal(
			t,
			"a042003b39e5d153ba6bce431effe7d2155adfadc46af9f30ac631f970d570e2",
			digest.MustNewDigest(
				"hello",
				remoteexecution.DigestFunction_SHA256TREE,
				"a042003b39e5d153ba6bce431effe7d2155adfadc46af9f30ac631f970d570e2",
				123).GetHashString())
	})
}

func TestDigestGetSizeBytes(t *testing.T) {
	require.Equal(
		t,
		int64(123),
		digest.MustNewDigest(
			"hello",
			remoteexecution.DigestFunction_SHA256,
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetSizeBytes())
}

func TestDigestGetKey(t *testing.T) {
	t.Run("SHA256", func(t *testing.T) {
		d := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 123)
		require.Equal(
			t,
			"1-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-123",
			d.GetKey(digest.KeyWithoutInstance))
		require.Equal(
			t,
			"1-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-123-hello",
			d.GetKey(digest.KeyWithInstance))
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		d := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256TREE, "5d8242df5726318bec51ccc6166a284ce40850cb7e9f4b041ce3df8a7fa61dc4", 123)
		require.Equal(
			t,
			"8-5d8242df5726318bec51ccc6166a284ce40850cb7e9f4b041ce3df8a7fa61dc4-123",
			d.GetKey(digest.KeyWithoutInstance))
		require.Equal(
			t,
			"8-5d8242df5726318bec51ccc6166a284ce40850cb7e9f4b041ce3df8a7fa61dc4-123-hello",
			d.GetKey(digest.KeyWithInstance))
	})
}

func TestDigestString(t *testing.T) {
	require.Equal(
		t,
		"1-e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-123-hello",
		digest.MustNewDigest(
			"hello",
			remoteexecution.DigestFunction_SHA256,
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).String())
}

func TestDigestToSingletonSet(t *testing.T) {
	d := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 123)
	require.Equal(
		t,
		digest.NewSetBuilder().Add(d).Build(),
		d.ToSingletonSet())
}

func TestKeyFormatCombine(t *testing.T) {
	// If one of the two backends requires that digests are keyed
	// with instance names in place, that format should be used
	// externally as well.
	require.Equal(t, digest.KeyWithoutInstance, digest.KeyWithoutInstance.Combine(digest.KeyWithoutInstance))
	require.Equal(t, digest.KeyWithInstance, digest.KeyWithoutInstance.Combine(digest.KeyWithInstance))
	require.Equal(t, digest.KeyWithInstance, digest.KeyWithInstance.Combine(digest.KeyWithoutInstance))
	require.Equal(t, digest.KeyWithInstance, digest.KeyWithInstance.Combine(digest.KeyWithInstance))
}

func TestDigestGetDigestsWithParentInstanceNames(t *testing.T) {
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())

	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())

	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())

	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello/world", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello/world/cup", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("hello/world/cup", remoteexecution.DigestFunction_MD5, "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())
}

func TestRemoveUnsupportedDigestFunctions(t *testing.T) {
	require.Equal(
		t,
		[]remoteexecution.DigestFunction_Value{
			remoteexecution.DigestFunction_MD5,
			remoteexecution.DigestFunction_SHA1,
			remoteexecution.DigestFunction_SHA256,
		},
		digest.RemoveUnsupportedDigestFunctions([]remoteexecution.DigestFunction_Value{
			remoteexecution.DigestFunction_MD5,
			remoteexecution.DigestFunction_SHA256,
			remoteexecution.DigestFunction_SHA1,
			remoteexecution.DigestFunction_SHA1,
			remoteexecution.DigestFunction_VSO,
		}))
}

func TestDigestGetCompactBinary(t *testing.T) {
	t.Run("SHA256", func(t *testing.T) {
		d := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "18c17f53df2fcd1f8271bc1c0e55df71b1a796eaa74ff45a68900f04e3f4c7a2", 124982395)
		require.Equal(
			t,
			[]byte{
				// Digest function: remoteexecution.DigestFunction_SHA256.
				0x01,
				// Hash.
				0x18, 0xc1, 0x7f, 0x53, 0xdf, 0x2f, 0xcd, 0x1f,
				0x82, 0x71, 0xbc, 0x1c, 0x0e, 0x55, 0xdf, 0x71,
				0xb1, 0xa7, 0x96, 0xea, 0xa7, 0x4f, 0xf4, 0x5a,
				0x68, 0x90, 0x0f, 0x04, 0xe3, 0xf4, 0xc7, 0xa2,
				// Size.
				0xf6, 0xd1, 0x98, 0x77,
			},
			d.GetCompactBinary())
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		d := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256TREE, "afc6637b936b0e07d9aea351530a75e390d0216582596edb45e208ad7de96b35", 124982395)
		require.Equal(
			t,
			[]byte{
				// Digest function: remoteexecution.DigestFunction_SHA256TREE.
				0x08,
				// Hash.
				0xaf, 0xc6, 0x63, 0x7b, 0x93, 0x6b, 0x0e, 0x07,
				0xd9, 0xae, 0xa3, 0x51, 0x53, 0x0a, 0x75, 0xe3,
				0x90, 0xd0, 0x21, 0x65, 0x82, 0x59, 0x6e, 0xdb,
				0x45, 0xe2, 0x08, 0xad, 0x7d, 0xe9, 0x6b, 0x35,
				// Size.
				0xf6, 0xd1, 0x98, 0x77,
			},
			d.GetCompactBinary())
	})
}
