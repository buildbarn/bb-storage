package digest_test

import (
	"testing"

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
		require.Equal(t, digest.MustNewInstanceName(""), instanceName)
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
