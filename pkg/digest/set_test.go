package digest_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestSetEmpty(t *testing.T) {
	require.True(t, digest.EmptySet.Empty())
	require.False(
		t,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Build().Empty())
}

func TestSetFirst(t *testing.T) {
	_, ok := digest.EmptySet.First()
	require.False(t, ok)

	d, ok := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
		Build().First()
	require.True(t, ok)
	require.Equal(t, digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123), d)
}

func TestSetLength(t *testing.T) {
	require.Equal(t, 0, digest.EmptySet.Length())
	require.Equal(
		t,
		1,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Build().Length())
	require.Equal(
		t,
		2,
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Add(digest.MustNewDigest("instance", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 123)).
			Build().Length())
}

func TestGetDifferenceAndIntersection(t *testing.T) {
	onlyA, both, onlyB := digest.GetDifferenceAndIntersection(
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Add(digest.MustNewDigest("instance", "1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123)).
			Add(digest.MustNewDigest("instance", "0fffffffffffffffffffffffffffffff", 789)).
			Add(digest.MustNewDigest("instance", "1fffffffffffffffffffffffffffffff", 789)).
			Build(),
		digest.NewSetBuilder().
			Add(digest.MustNewDigest("instance", "0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456)).
			Add(digest.MustNewDigest("instance", "1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456)).
			Add(digest.MustNewDigest("instance", "0fffffffffffffffffffffffffffffff", 789)).
			Add(digest.MustNewDigest("instance", "1fffffffffffffffffffffffffffffff", 789)).
			Build())

	// Ensure that the resulting sets both have the right contents,
	// while maintaining the correct sorting order.
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("instance", "0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123),
			digest.MustNewDigest("instance", "1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 123),
		},
		onlyA.Items())
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("instance", "0fffffffffffffffffffffffffffffff", 789),
			digest.MustNewDigest("instance", "1fffffffffffffffffffffffffffffff", 789),
		},
		both.Items())
	require.Equal(
		t,
		[]digest.Digest{
			digest.MustNewDigest("instance", "0bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456),
			digest.MustNewDigest("instance", "1bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 456),
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
				digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
				digest.MustNewDigest("instance", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1),
			},
			digest.GetUnion([]digest.Set{
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)).
					Add(digest.MustNewDigest("instance", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1)).
					Build(),
			}).Items())
	})

	t.Run("Complex", func(t *testing.T) {
		// Three-way merge.
		require.Equal(
			t,
			[]digest.Digest{
				digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
				digest.MustNewDigest("instance", "abababababababababababababababab", 2),
				digest.MustNewDigest("instance", "abcabcabcabcabcabcabcabcabcabcab", 3),
				digest.MustNewDigest("instance", "acacacacacacacacacacacacacacacac", 2),
				digest.MustNewDigest("instance", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1),
				digest.MustNewDigest("instance", "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc", 2),
				digest.MustNewDigest("instance", "cccccccccccccccccccccccccccccccc", 1),
			},
			digest.GetUnion([]digest.Set{
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)).
					Add(digest.MustNewDigest("instance", "abababababababababababababababab", 2)).
					Add(digest.MustNewDigest("instance", "acacacacacacacacacacacacacacacac", 2)).
					Add(digest.MustNewDigest("instance", "abcabcabcabcabcabcabcabcabcabcab", 3)).
					Build(),
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 1)).
					Add(digest.MustNewDigest("instance", "abababababababababababababababab", 2)).
					Add(digest.MustNewDigest("instance", "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc", 2)).
					Add(digest.MustNewDigest("instance", "abcabcabcabcabcabcabcabcabcabcab", 3)).
					Build(),
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("instance", "cccccccccccccccccccccccccccccccc", 1)).
					Add(digest.MustNewDigest("instance", "acacacacacacacacacacacacacacacac", 2)).
					Add(digest.MustNewDigest("instance", "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc", 2)).
					Add(digest.MustNewDigest("instance", "abcabcabcabcabcabcabcabcabcabcab", 3)).
					Build(),
			}).Items())
	})
}
