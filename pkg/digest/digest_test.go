package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewDigestFromByteStreamReadPath(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"))
	})

	t.Run("BlabsInsteadOfBlobs", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("blabs/8b1a9953c4611296a827abf8c47804d7/123")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"))
	})

	t.Run("NonIntegerSize", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("blobs/8b1a9953c4611296a827abf8c47804d7/five")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid blob size \"five\""))
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("x/operations/y/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid instance name \"x/operations/y\": Instance name contains reserved keyword \"operations\""))
	})

	t.Run("UnknownCompressionMethod", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamReadPath("x/compressed-blobs/xyzzy/8b1a9953c4611296a827abf8c47804d7/123")
		require.Equal(t, err, status.Error(codes.Unimplemented, "Unsupported compression scheme \"xyzzy\""))
	})

	t.Run("NoInstanceName", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/world/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("RedundantSlashes", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("//hello//world//blobs//8b1a9953c4611296a827abf8c47804d7//123//")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("Zstandard", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/world/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_ZSTD, compressor)
	})

	t.Run("Deflate", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamReadPath("hello/world/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_DEFLATE, compressor)
	})
}

func TestNewDigestFromByteStreamWritePath(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"))
	})

	t.Run("DownloadsInsteadOfUploads", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("downloads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid resource naming scheme"))
	})

	t.Run("NonIntegerSize", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/five")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid blob size \"five\""))
	})

	t.Run("InvalidInstanceName", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("x/operations/y/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.Equal(t, err, status.Error(codes.InvalidArgument, "Invalid instance name \"x/operations/y\": Instance name contains reserved keyword \"operations\""))
	})

	t.Run("UnknownCompressionMethod", func(t *testing.T) {
		_, _, err := digest.NewDigestFromByteStreamWritePath("x/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/compressed-blobs/xyzzy/8b1a9953c4611296a827abf8c47804d7/123")
		require.Equal(t, err, status.Error(codes.Unimplemented, "Unsupported compression scheme \"xyzzy\""))
	})

	t.Run("NoInstanceName", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("RedundantSlashes", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("//hello//world//uploads//da2f1135-326b-4956-b920-1646cdd6cb53//blobs//8b1a9953c4611296a827abf8c47804d7//123//")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("TrailingPath", func(t *testing.T) {
		// Upload paths may contain a trailing filename that the
		// implementation can use to attach a name to the
		// object. This implementation ignores that information.
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/blobs/8b1a9953c4611296a827abf8c47804d7/123/this/file/is/called/foo.txt")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_IDENTITY, compressor)
	})

	t.Run("Zstandard", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_ZSTD, compressor)
	})

	t.Run("Deflate", func(t *testing.T) {
		d, compressor, err := digest.NewDigestFromByteStreamWritePath("hello/world/uploads/da2f1135-326b-4956-b920-1646cdd6cb53/compressed-blobs/deflate/8b1a9953c4611296a827abf8c47804d7/123")
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello/world", "8b1a9953c4611296a827abf8c47804d7", 123), d)
		require.Equal(t, remoteexecution.Compressor_DEFLATE, compressor)
	})
}

func TestDigestGetByteStreamReadPath(t *testing.T) {
	t.Run("NoInstanceName", func(t *testing.T) {
		require.Equal(
			t,
			"blobs/8b1a9953c4611296a827abf8c47804d7/123",
			digest.MustNewDigest(
				"",
				"8b1a9953c4611296a827abf8c47804d7",
				123).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		require.Equal(
			t,
			"hello/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			digest.MustNewDigest(
				"hello",
				"8b1a9953c4611296a827abf8c47804d7",
				123).GetByteStreamReadPath(remoteexecution.Compressor_IDENTITY))
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d := digest.MustNewDigest(
			"hello/world",
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
		require.Equal(
			t,
			"uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			digest.MustNewDigest(
				"",
				"8b1a9953c4611296a827abf8c47804d7",
				123).GetByteStreamWritePath(uuid, remoteexecution.Compressor_IDENTITY))
	})

	t.Run("InstanceNameOneComponent", func(t *testing.T) {
		require.Equal(
			t,
			"hello/uploads/36ebab65-3c4f-4faf-818b-2eabb4cd1b02/blobs/8b1a9953c4611296a827abf8c47804d7/123",
			digest.MustNewDigest(
				"hello",
				"8b1a9953c4611296a827abf8c47804d7",
				123).GetByteStreamWritePath(uuid, remoteexecution.Compressor_IDENTITY))
	})

	t.Run("InstanceNameTwoComponents", func(t *testing.T) {
		d := digest.MustNewDigest(
			"hello/world",
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
	require.Equal(
		t,
		&remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 123,
		},
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetProto())
}

func TestDigestGetInstanceName(t *testing.T) {
	require.Equal(
		t,
		digest.MustNewInstanceName("hello"),
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetInstanceName())
}

func TestDigestGetHashBytes(t *testing.T) {
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
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetHashBytes())
}

func TestDigestGetHashString(t *testing.T) {
	require.Equal(
		t,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetHashString())
}

func TestDigestGetSizeBytes(t *testing.T) {
	require.Equal(
		t,
		int64(123),
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetSizeBytes())
}

func TestDigestGetKey(t *testing.T) {
	d := digest.MustNewDigest("hello", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 123)
	require.Equal(
		t,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-123",
		d.GetKey(digest.KeyWithoutInstance))
	require.Equal(
		t,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-123-hello",
		d.GetKey(digest.KeyWithInstance))
}

func TestDigestString(t *testing.T) {
	require.Equal(
		t,
		"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-123-hello",
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).String())
}

func TestDigestToSingletonSet(t *testing.T) {
	d := digest.MustNewDigest("hello", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 123)
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
			digest.MustNewDigest("", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("", "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())

	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("hello", "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())

	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello/world", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("hello/world", "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())

	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello/world", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
			digest.MustNewDigest("hello/world/cup", "3d6b0f4e4ba25243c43e045dfe23845a", 123),
		},
		digest.MustNewDigest("hello/world/cup", "3d6b0f4e4ba25243c43e045dfe23845a", 123).GetDigestsWithParentInstanceNames())
}
