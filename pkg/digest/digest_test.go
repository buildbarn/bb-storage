package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNewDigest(t *testing.T) {
	_, err := digest.NewDigest("hello", "0123456789abcd", 123)
	require.Equal(t, status.Error(codes.InvalidArgument, "Unknown digest hash length: 14 characters"), err)

	_, err = digest.NewDigest("hello", "555555555555555X5555555555555555", 123)
	require.Equal(t, status.Error(codes.InvalidArgument, "Non-hexadecimal character in digest hash: U+0058 'X'"), err)

	_, err = digest.NewDigest("hello", "00000000000000000000000000000000", -1)
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid digest size: -1 bytes"), err)
}

func TestDigestGetPartialDigest(t *testing.T) {
	require.Equal(
		t,
		&remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 123,
		},
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetPartialDigest())
}

func TestDigestGetInstance(t *testing.T) {
	require.Equal(
		t,
		"hello",
		digest.MustNewDigest(
			"hello",
			"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			123).GetInstance())
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
