package digest_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestSetBuilderEmpty(t *testing.T) {
	// For unit testing purposes, empty sets created through
	// SetBuilder must be deeply equal to EmptySet. This means that
	// the slice of digests stored within is nil.
	//
	// This also cuts down the number of memory allocations. It is
	// fairly common for BlobAccess.FindMissing() to return empty
	// sets. Letting those use an additional allocation would be
	// wasteful.
	require.Equal(t, digest.EmptySet, digest.NewSetBuilder(0).Build())
}

func TestSetBuilderCapacity(t *testing.T) {
	// The capacity argument is purely a sizing optimization. The
	// resulting Set must be indistinguishable regardless of the
	// capacity hint, including the EmptySet invariant when no
	// elements are added.
	t.Run("Empty", func(t *testing.T) {
		require.Equal(t, digest.EmptySet, digest.NewSetBuilder(0).Build())
		require.Equal(t, digest.EmptySet, digest.NewSetBuilder(100).Build())
	})

	t.Run("Equivalence", func(t *testing.T) {
		digests := []digest.Digest{
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "cccccccccccccccccccccccccccccccc", 3),
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1),
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2),
		}
		expected := digest.NewSetBuilder(0)
		actual := digest.NewSetBuilder(len(digests))
		for _, d := range digests {
			expected.Add(d)
			actual.Add(d)
		}
		require.Equal(t, expected.Build(), actual.Build())
	})
}

func TestSetBuilderOrdering(t *testing.T) {
	// Build() must return digests in a stable, sorted order
	// regardless of insertion order, so that Set comparison
	// operations such as GetDifferenceAndIntersection() and
	// GetUnion() can be implemented in linear time.
	d1 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1)
	d2 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", 2)
	d3 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "cccccccccccccccccccccccccccccccc", 3)
	d4 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "dddddddddddddddddddddddddddddddd", 4)
	want := []digest.Digest{d1, d2, d3, d4}

	t.Run("ReverseOrder", func(t *testing.T) {
		require.Equal(t, want, digest.NewSetBuilder(0).Add(d4).Add(d3).Add(d2).Add(d1).Build().Items())
	})

	t.Run("ShuffledOrder", func(t *testing.T) {
		require.Equal(t, want, digest.NewSetBuilder(0).Add(d2).Add(d4).Add(d1).Add(d3).Build().Items())
	})

	t.Run("Duplicates", func(t *testing.T) {
		require.Equal(t, want, digest.NewSetBuilder(0).Add(d3).Add(d1).Add(d3).Add(d2).Add(d4).Add(d1).Build().Items())
	})
}
