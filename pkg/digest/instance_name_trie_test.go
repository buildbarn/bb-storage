package digest_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestInstanceNameTrie(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		// Attempt to perform lookups on an empty trie.
		it := digest.NewInstanceNameTrie()

		require.Equal(t, -1, it.Get(digest.EmptyInstanceName))
		require.False(t, it.Contains(digest.EmptyInstanceName))

		require.Equal(t, -1, it.Get(digest.MustNewInstanceName("hello")))
		require.False(t, it.Contains(digest.MustNewInstanceName("hello")))

		require.Equal(t, -1, it.Get(digest.MustNewInstanceName("hello/world")))
		require.False(t, it.Contains(digest.MustNewInstanceName("hello/world")))

	})

	t.Run("WithoutRoot", func(t *testing.T) {
		// Attempt to perform lookups on a trie that does not
		// have a root node.
		it := digest.NewInstanceNameTrie()
		it.Set(digest.MustNewInstanceName("a"), 123)
		it.Set(digest.MustNewInstanceName("a/b/c"), 456)
		it.Set(digest.MustNewInstanceName("a/b/c/d/e"), 789)

		require.Equal(t, -1, it.Get(digest.EmptyInstanceName))
		require.False(t, it.Contains(digest.EmptyInstanceName))

		require.Equal(t, 123, it.Get(digest.MustNewInstanceName("a")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a")))

		require.Equal(t, 123, it.Get(digest.MustNewInstanceName("a/b")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b")))

		require.Equal(t, 456, it.Get(digest.MustNewInstanceName("a/b/c")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c")))

		require.Equal(t, 456, it.Get(digest.MustNewInstanceName("a/b/c/d")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c/d")))

		require.Equal(t, 789, it.Get(digest.MustNewInstanceName("a/b/c/d/e")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c/d/e")))

		require.Equal(t, 789, it.Get(digest.MustNewInstanceName("a/b/c/d/e/f")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c/d/e/f")))
	})

	t.Run("WithRoot", func(t *testing.T) {
		// Attempt to perform lookups on a trie that has a root node.
		it := digest.NewInstanceNameTrie()
		it.Set(digest.EmptyInstanceName, 123)
		it.Set(digest.MustNewInstanceName("a/b"), 456)
		it.Set(digest.MustNewInstanceName("a/b/c/d"), 789)

		require.Equal(t, 123, it.Get(digest.EmptyInstanceName))
		require.True(t, it.Contains(digest.EmptyInstanceName))

		require.Equal(t, 123, it.Get(digest.MustNewInstanceName("a")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a")))

		require.Equal(t, 456, it.Get(digest.MustNewInstanceName("a/b")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b")))

		require.Equal(t, 456, it.Get(digest.MustNewInstanceName("a/b/c")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c")))

		require.Equal(t, 789, it.Get(digest.MustNewInstanceName("a/b/c/d")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c/d")))

		require.Equal(t, 789, it.Get(digest.MustNewInstanceName("a/b/c/d/e")))
		require.True(t, it.Contains(digest.MustNewInstanceName("a/b/c/d/e")))
	})
}
