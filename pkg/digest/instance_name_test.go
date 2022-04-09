package digest_test

import (
	"bytes"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewInstanceName(t *testing.T) {
	t.Run("RedundantSlashes", func(t *testing.T) {
		_, err := digest.NewInstanceName("/")
		require.Equal(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)

		_, err = digest.NewInstanceName("/hello")
		require.Equal(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)

		_, err = digest.NewInstanceName("hello/")
		require.Equal(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)

		_, err = digest.NewInstanceName("hello//world")
		require.Equal(t, status.Error(codes.InvalidArgument, "Instance name contains redundant slashes"), err)
	})

	t.Run("ReservedKeyword", func(t *testing.T) {
		_, err := digest.NewInstanceName("keyword/blobs/is/reserved")
		require.Equal(t, status.Error(codes.InvalidArgument, "Instance name contains reserved keyword \"blobs\""), err)
	})

	t.Run("Success", func(t *testing.T) {
		instanceName, err := digest.NewInstanceName("")
		require.NoError(t, err)
		require.Equal(t, digest.EmptyInstanceName, instanceName)
	})
}

func TestInstanceNameNewDigest(t *testing.T) {
	instanceName := digest.MustNewInstanceName("hello")

	_, err := instanceName.NewDigest("0123456789abcd", 123)
	require.Equal(t, status.Error(codes.InvalidArgument, "Unknown digest hash length: 14 characters"), err)

	_, err = instanceName.NewDigest("555555555555555X5555555555555555", 123)
	require.Equal(t, status.Error(codes.InvalidArgument, "Non-hexadecimal character in digest hash: U+0058 'X'"), err)

	_, err = instanceName.NewDigest("00000000000000000000000000000000", -1)
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid digest size: -1 bytes"), err)
}

func TestInstanceNameGetDigestFunction(t *testing.T) {
	instanceName := digest.MustNewInstanceName("hello")

	t.Run("UnknownDigestFunction", func(t *testing.T) {
		_, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_UNKNOWN)
		require.Equal(t, status.Error(codes.InvalidArgument, "Unknown digest function"), err)
	})

	t.Run("MD5", func(t *testing.T) {
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_MD5)
		require.NoError(t, err)

		g := digestFunction.NewGenerator()
		g.Write([]byte("Hello"))
		require.Equal(t, digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5), g.Sum())

		require.True(t, digest.MustNewDigest("hello", "ff9cecc701d5f6c1e45d5163a4cf850a", 123).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("bye", "74979421339434acb78d07ad44754015", 456).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("hello", "5ad9e0fd2f11ec59c95c60020c2b00afbef10e5b", 789).UsesDigestFunction(digestFunction))
	})

	t.Run("SHA-1", func(t *testing.T) {
		digestFunction, err := instanceName.GetDigestFunction(remoteexecution.DigestFunction_SHA1)
		require.NoError(t, err)

		g := digestFunction.NewGenerator()
		g.Write([]byte("Hello"))
		require.Equal(t, digest.MustNewDigest("hello", "f7ff9e8b7bb2e09b70935a5d785e0cc5d9d0abf0", 5), g.Sum())

		require.True(t, digest.MustNewDigest("hello", "b407b10e52b2bddee20be9e475c8755d9de67473", 123).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("bye", "f11999245771a5c184b62dc5380e0d8b42df67b4", 456).UsesDigestFunction(digestFunction))
		require.False(t, digest.MustNewDigest("hello", "1f69e2d170a0ada2b853fe2adc6d1c47", 789).UsesDigestFunction(digestFunction))
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

	blobDigest, err := instanceName.NewDigestFromCompactBinary(bytes.NewBuffer([]byte{
		// Length of hash.
		0x20,
		// Hash.
		0x18, 0xc1, 0x7f, 0x53, 0xdf, 0x2f, 0xcd, 0x1f,
		0x82, 0x71, 0xbc, 0x1c, 0x0e, 0x55, 0xdf, 0x71,
		0xb1, 0xa7, 0x96, 0xea, 0xa7, 0x4f, 0xf4, 0x5a,
		0x68, 0x90, 0x0f, 0x04, 0xe3, 0xf4, 0xc7, 0xa2,
		// Size.
		0xf6, 0xd1, 0x98, 0x77,
	}))
	require.NoError(t, err)
	require.Equal(t, digest.MustNewDigest("hello", "18c17f53df2fcd1f8271bc1c0e55df71b1a796eaa74ff45a68900f04e3f4c7a2", 124982395), blobDigest)
}
