package digest_test

import (
	"bytes"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewInstanceName(t *testing.T) {
	t.Run("RedundantSlashes", func(t *testing.T) {
		_, err := digest.NewInstanceName("/")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)

		_, err = digest.NewInstanceName("/hello")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)

		_, err = digest.NewInstanceName("hello/")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)

		_, err = digest.NewInstanceName("hello//world")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)
	})

	t.Run("ReservedKeyword", func(t *testing.T) {
		_, err := digest.NewInstanceName("keyword/blobs/is/reserved")
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name contains reserved keyword \"blobs\""), err)
	})

	t.Run("Success", func(t *testing.T) {
		instanceName, err := digest.NewInstanceName("")
		require.NoError(t, err)
		require.Equal(t, digest.EmptyInstanceName, instanceName)
	})
}

func TestInstanceNameGetDigestFunction(t *testing.T) {
	instanceName := digest.MustNewInstanceName("hello")

	t.Run("UnknownDigestFunction", func(t *testing.T) {
		_, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_UNKNOWN, 0)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unknown digest function"), err)
	})

	t.Run("MD5", func(t *testing.T) {
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_MD5, 0)
		require.NoError(t, err)

		g := digestFunction.NewGenerator(5)
		g.Write([]byte("Hello"))
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5), g.Sum())

		require.True(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "ff9cecc701d5f6c1e45d5163a4cf850a", 123).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("bye", remoteexecution.DigestFunction_MD5, "74979421339434acb78d07ad44754015", 456).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA1, "5ad9e0fd2f11ec59c95c60020c2b00afbef10e5b", 789).UsesDigestFunction(digestFunction))
	})

	t.Run("SHA-1", func(t *testing.T) {
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA1, 0)
		require.NoError(t, err)

		g := digestFunction.NewGenerator(5)
		g.Write([]byte("Hello"))
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA1, "f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0", 5), g.Sum())

		require.True(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA1, "b407b10e52b2bddee20be9e475c8755d9de67473", 123).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("bye", remoteexecution.DigestFunction_SHA1, "f11999245771a5c184b62dc5380e0d8b42df67b4", 456).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1f69e2d170a0ada2b853fe2adc6d1c47", 789).UsesDigestFunction(digestFunction))
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA256TREE, 0)
		require.NoError(t, err)

		g := digestFunction.NewGenerator(5)
		g.Write([]byte("Hello"))
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256TREE, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5), g.Sum())

		require.True(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256TREE, "c1b1c3e4000faffe4c9f325a251554a19442b3cd8f5c5b80ce34d9cad257fcd7", 123).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("bye", remoteexecution.DigestFunction_SHA256TREE, "c1b1c3e4000faffe4c9f325a251554a19442b3cd8f5c5b80ce34d9cad257fcd7", 456).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA1, "5ad9e0fd2f11ec59c95c60020c2b00afbef10e5b", 789).UsesDigestFunction(digestFunction))
	})
}

func TestInstanceNameGetComponents(t *testing.T) {
	require.Empty(t, digest.EmptyInstanceName.GetComponents())

	require.Equal(
		t,
		[]string{"hello"},
		digest.MustNewInstanceName("hello").GetComponents())

	require.Equal(
		t,
		[]string{"hello", "world"},
		digest.MustNewInstanceName("hello/world").GetComponents())
}

func TestInstanceNameNewDigestFromCompactBinary(t *testing.T) {
	instanceName := digest.MustNewInstanceName("hello")

	t.Run("SHA256", func(t *testing.T) {
		blobDigest, err := instanceName.NewDigestFromCompactBinary(bytes.NewBuffer([]byte{
			// Digest function: remoteexecution.DigestFunction_SHA256.
			0x01,
			// Hash.
			0x18, 0xc1, 0x7f, 0x53, 0xdf, 0x2f, 0xcd, 0x1f,
			0x82, 0x71, 0xbc, 0x1c, 0x0e, 0x55, 0xdf, 0x71,
			0xb1, 0xa7, 0x96, 0xea, 0xa7, 0x4f, 0xf4, 0x5a,
			0x68, 0x90, 0x0f, 0x04, 0xe3, 0xf4, 0xc7, 0xa2,
			// Size.
			0xf6, 0xd1, 0x98, 0x77,
		}))
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "18c17f53df2fcd1f8271bc1c0e55df71b1a796eaa74ff45a68900f04e3f4c7a2", 124982395), blobDigest)
	})

	t.Run("SHA256TREE", func(t *testing.T) {
		blobDigest, err := instanceName.NewDigestFromCompactBinary(bytes.NewBuffer([]byte{
			// Digest function: remoteexecution.DigestFunction_SHA256TREE.
			0x08,
			// Hash.
			0x59, 0xd3, 0xf9, 0x91, 0x4a, 0x2e, 0x32, 0x0b,
			0x1b, 0x36, 0xe1, 0x18, 0x0f, 0xb6, 0x81, 0xfe,
			0xe9, 0xc4, 0x34, 0xa2, 0x13, 0xc0, 0xaf, 0xbd,
			0x03, 0x93, 0xba, 0xc3, 0xf9, 0x37, 0xd0, 0xc8,
			// Size.
			0xf6, 0xd1, 0x98, 0x77,
		}))
		require.NoError(t, err)
		require.Equal(t, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256TREE, "59d3f9914a2e320b1b36e1180fb681fee9c434a213c0afbd0393bac3f937d0c8", 124982395), blobDigest)
	})
}
