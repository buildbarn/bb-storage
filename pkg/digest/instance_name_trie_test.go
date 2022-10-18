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

		require.Equal(t, -1, it.GetExact(digest.EmptyInstanceName))
		require.False(t, it.ContainsExact(digest.EmptyInstanceName))
		require.Equal(t, -1, it.GetLongestPrefix(digest.EmptyInstanceName))
		require.False(t, it.ContainsPrefix(digest.EmptyInstanceName))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("hello")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("hello")))
		require.Equal(t, -1, it.GetLongestPrefix(digest.MustNewInstanceName("hello")))
		require.False(t, it.ContainsPrefix(digest.MustNewInstanceName("hello")))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("hello/world")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("hello/world")))
		require.Equal(t, -1, it.GetLongestPrefix(digest.MustNewInstanceName("hello/world")))
		require.False(t, it.ContainsPrefix(digest.MustNewInstanceName("hello/world")))
	})

	t.Run("WithoutRoot", func(t *testing.T) {
		// Attempt to perform lookups on a trie that does not
		// have a root node.
		it := digest.NewInstanceNameTrie()
		it.Set(digest.MustNewInstanceName("a"), 123)
		it.Set(digest.MustNewInstanceName("a/b/c"), 456)
		it.Set(digest.MustNewInstanceName("a/b/c/d/e"), 789)

		require.Equal(t, -1, it.GetExact(digest.EmptyInstanceName))
		require.False(t, it.ContainsExact(digest.EmptyInstanceName))
		require.Equal(t, -1, it.GetLongestPrefix(digest.EmptyInstanceName))
		require.False(t, it.ContainsPrefix(digest.EmptyInstanceName))

		require.Equal(t, 123, it.GetExact(digest.MustNewInstanceName("a")))
		require.True(t, it.ContainsExact(digest.MustNewInstanceName("a")))
		require.Equal(t, 123, it.GetLongestPrefix(digest.MustNewInstanceName("a")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a")))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("a/b")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("a/b")))
		require.Equal(t, 123, it.GetLongestPrefix(digest.MustNewInstanceName("a/b")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b")))

		require.Equal(t, 456, it.GetExact(digest.MustNewInstanceName("a/b/c")))
		require.True(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c")))
		require.Equal(t, 456, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c")))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("a/b/c/d")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c/d")))
		require.Equal(t, 456, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d")))

		require.Equal(t, 789, it.GetExact(digest.MustNewInstanceName("a/b/c/d/e")))
		require.True(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c/d/e")))
		require.Equal(t, 789, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d/e")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d/e")))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("a/b/c/d/e/f")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c/d/e/f")))
		require.Equal(t, 789, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d/e/f")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d/e/f")))

		// Removing all entries should make all instance names
		// unresolvable.
		require.False(t, it.Remove(digest.MustNewInstanceName("a/b/c/d/e")))
		require.False(t, it.Remove(digest.MustNewInstanceName("a")))
		require.True(t, it.Remove(digest.MustNewInstanceName("a/b/c")))

		require.Equal(t, -1, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d/e/f")))
		require.False(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d/e/f")))
	})

	t.Run("WithRoot", func(t *testing.T) {
		// Attempt to perform lookups on a trie that has a root node.
		it := digest.NewInstanceNameTrie()
		it.Set(digest.EmptyInstanceName, 123)
		it.Set(digest.MustNewInstanceName("a/b"), 456)
		it.Set(digest.MustNewInstanceName("a/b/c/d"), 789)

		require.Equal(t, 123, it.GetExact(digest.EmptyInstanceName))
		require.True(t, it.ContainsExact(digest.EmptyInstanceName))
		require.Equal(t, 123, it.GetLongestPrefix(digest.EmptyInstanceName))
		require.True(t, it.ContainsPrefix(digest.EmptyInstanceName))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("a")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("a")))
		require.Equal(t, 123, it.GetLongestPrefix(digest.MustNewInstanceName("a")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a")))

		require.Equal(t, 456, it.GetExact(digest.MustNewInstanceName("a/b")))
		require.True(t, it.ContainsExact(digest.MustNewInstanceName("a/b")))
		require.Equal(t, 456, it.GetLongestPrefix(digest.MustNewInstanceName("a/b")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b")))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("a/b/c")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c")))
		require.Equal(t, 456, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c")))

		require.Equal(t, 789, it.GetExact(digest.MustNewInstanceName("a/b/c/d")))
		require.True(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c/d")))
		require.Equal(t, 789, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d")))

		require.Equal(t, -1, it.GetExact(digest.MustNewInstanceName("a/b/c/d/e")))
		require.False(t, it.ContainsExact(digest.MustNewInstanceName("a/b/c/d/e")))
		require.Equal(t, 789, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d/e")))
		require.True(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d/e")))

		// Removing all entries should make all instance names
		// unresolvable.
		require.False(t, it.Remove(digest.EmptyInstanceName))
		require.False(t, it.Remove(digest.MustNewInstanceName("a/b")))
		require.True(t, it.Remove(digest.MustNewInstanceName("a/b/c/d")))

		require.Equal(t, -1, it.GetLongestPrefix(digest.MustNewInstanceName("a/b/c/d/e")))
		require.False(t, it.ContainsPrefix(digest.MustNewInstanceName("a/b/c/d/e")))
	})
}
