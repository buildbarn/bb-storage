package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestSetEmpty(t *testing.T) {
	require.True(t, digest.EmptySet.Empty())
	require.False(
		t,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Build().Empty())
}

func TestSetFirst(t *testing.T) {
	_, ok := digest.EmptySet.First()
	require.False(t, ok)

	d, ok := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
		Build().First()
	require.True(t, ok)
	require.Equal(t, digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123), d)
}

func TestSetLength(t *testing.T) {
	require.Equal(t, 0, digest.EmptySet.Length())
	require.Equal(
		t,
		1,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Build().Length())
	require.Equal(
		t,
		2,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 123)).
			Build().Length())
}

func TestSetRemoveEmptyBlob(t *testing.T) {
	require.Equal(t, digest.EmptySet, digest.EmptySet.RemoveEmptyBlob())

	// Set consisting entirely of empty blobs.
	require.Equal(
		t,
		digest.EmptySet,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_SHA1, "da39a3ee5e6b4b0d3255bfef95601890afd80709", 0)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)).
			Build().
			RemoveEmptyBlob())

	// Set consisting entirely of non-empty blobs.
	require.Equal(
		t,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d80d8a581e9e2b78fd2f5d990d0f0e21", 13)).
			Build(),
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d80d8a581e9e2b78fd2f5d990d0f0e21", 13)).
			Build().
			RemoveEmptyBlob())

	// Set consisting of both empty and non-empty blobs.
	require.Equal(
		t,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d80d8a581e9e2b78fd2f5d990d0f0e21", 13)).
			Build(),
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 11)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d80d8a581e9e2b78fd2f5d990d0f0e21", 13)).
			Build().
			RemoveEmptyBlob())
}

func TestPartitionByInstanceName(t *testing.T) {
	require.Empty(t, digest.EmptySet.PartitionByInstanceName())

	singleInstanceName := digest.NewSetBuilder().
		Add(digest.MustNewDigest("foo", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)).
		Add(digest.MustNewDigest("foo", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2)).
		Add(digest.MustNewDigest("foo", remoteexecution.DigestFunction_MD5, "cccccccccccccccccccccccccccccccc", 3)).
		Add(digest.MustNewDigest("foo", remoteexecution.DigestFunction_MD5, "dddddddddddddddddddddddddddddddd", 4)).
		Build()
	require.Equal(t, []digest.Set{singleInstanceName}, singleInstanceName.PartitionByInstanceName())

	require.Equal(
		t, []digest.Set{
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1)).
				Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "22222222222222222222222222222222", 2)).
				Build(),
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "33333333333333333333333333333333", 3)).
				Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "44444444444444444444444444444444", 4)).
				Build(),
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("c", remoteexecution.DigestFunction_MD5, "55555555555555555555555555555555", 5)).
				Add(digest.MustNewDigest("c", remoteexecution.DigestFunction_MD5, "66666666666666666666666666666666", 6)).
				Build(),
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("d", remoteexecution.DigestFunction_MD5, "77777777777777777777777777777777", 7)).
				Add(digest.MustNewDigest("d", remoteexecution.DigestFunction_MD5, "88888888888888888888888888888888", 8)).
				Build(),
		},
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "11111111111111111111111111111111", 1)).
			Add(digest.MustNewDigest("a", remoteexecution.DigestFunction_MD5, "22222222222222222222222222222222", 2)).
			Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "33333333333333333333333333333333", 3)).
			Add(digest.MustNewDigest("b", remoteexecution.DigestFunction_MD5, "44444444444444444444444444444444", 4)).
			Add(digest.MustNewDigest("c", remoteexecution.DigestFunction_MD5, "55555555555555555555555555555555", 5)).
			Add(digest.MustNewDigest("c", remoteexecution.DigestFunction_MD5, "66666666666666666666666666666666", 6)).
			Add(digest.MustNewDigest("d", remoteexecution.DigestFunction_MD5, "77777777777777777777777777777777", 7)).
			Add(digest.MustNewDigest("d", remoteexecution.DigestFunction_MD5, "88888888888888888888888888888888", 8)).
			Build().
			PartitionByInstanceName())
}

func TestGetDifferenceAndIntersection(t *testing.T) {
	onlyA, both, onlyB := digest.GetDifferenceAndIntersection(
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0fffffffffffffffffffffffffffffff", 789)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1fffffffffffffffffffffffffffffff", 789)).
			Build(),
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0fffffffffffffffffffffffffffffff", 789)).
			Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1fffffffffffffffffffffffffffffff", 789)).
			Build())

	// Ensure that the resulting sets both have the right contents,
	// while maintaining the correct sorting order.
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123),
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123),
		},
		onlyA.Items())
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0fffffffffffffffffffffffffffffff", 789),
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1fffffffffffffffffffffffffffffff", 789),
		},
		both.Items())
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456),
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456),
		},
		onlyB.Items())
}

func TestGetUnion(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		// No digests provided.
		require.Equal(t, digest.EmptySet, digest.GetUnion(nil))
		require.Equal(t, digest.EmptySet, digest.GetUnion([]digest.Set{digest.EmptySet}))
		require.Equal(t, digest.EmptySet, digest.GetUnion([]digest.Set{digest.EmptySet, digest.EmptySet}))

		// Single set with digests.
		require.Equal(
			t,
			[]digest.Digest{
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1),
			},
			digest.GetUnion([]digest.Set{
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1)).
					Build(),
			}).Items())
	})

	t.Run("Complex", func(t *testing.T) {
		// Three-way merge.
		require.Equal(
			t,
			[]digest.Digest{
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abababababababababababababababab", 2),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abcabcabcabcabcabcabcabcabcabcab", 3),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "acacacacacacacacacacacacacacacac", 2),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc", 2),
				digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "cccccccccccccccccccccccccccccccc", 1),
			},
			digest.GetUnion([]digest.Set{
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abababababababababababababababab", 2)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "acacacacacacacacacacacacacacacac", 2)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abcabcabcabcabcabcabcabcabcabcab", 3)).
					Build(),
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abababababababababababababababab", 2)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc", 2)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abcabcabcabcabcabcabcabcabcabcab", 3)).
					Build(),
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "cccccccccccccccccccccccccccccccc", 1)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "acacacacacacacacacacacacacacacac", 2)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc", 2)).
					Add(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "abcabcabcabcabcabcabcabcabcabcab", 3)).
					Build(),
			}).Items())
	})
}
