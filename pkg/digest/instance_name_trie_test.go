package digest_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
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

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("hello"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("hello"))))
		require.Equal(t, -1, it.GetLongestPrefix(util.Must(digest.NewInstanceName("hello"))))
		require.False(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("hello"))))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("hello/world"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("hello/world"))))
		require.Equal(t, -1, it.GetLongestPrefix(util.Must(digest.NewInstanceName("hello/world"))))
		require.False(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("hello/world"))))
	})

	t.Run("WithoutRoot", func(t *testing.T) {
		// Attempt to perform lookups on a trie that does not
		// have a root node.
		it := digest.NewInstanceNameTrie()
		it.Set(util.Must(digest.NewInstanceName("a")), 123)
		it.Set(util.Must(digest.NewInstanceName("a/b/c")), 456)
		it.Set(util.Must(digest.NewInstanceName("a/b/c/d/e")), 789)

		require.Equal(t, -1, it.GetExact(digest.EmptyInstanceName))
		require.False(t, it.ContainsExact(digest.EmptyInstanceName))
		require.Equal(t, -1, it.GetLongestPrefix(digest.EmptyInstanceName))
		require.False(t, it.ContainsPrefix(digest.EmptyInstanceName))

		require.Equal(t, 123, it.GetExact(util.Must(digest.NewInstanceName("a"))))
		require.True(t, it.ContainsExact(util.Must(digest.NewInstanceName("a"))))
		require.Equal(t, 123, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a"))))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("a/b"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b"))))
		require.Equal(t, 123, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b"))))

		require.Equal(t, 456, it.GetExact(util.Must(digest.NewInstanceName("a/b/c"))))
		require.True(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c"))))
		require.Equal(t, 456, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c"))))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("a/b/c/d"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c/d"))))
		require.Equal(t, 456, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d"))))

		require.Equal(t, 789, it.GetExact(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.True(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.Equal(t, 789, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e"))))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("a/b/c/d/e/f"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c/d/e/f"))))
		require.Equal(t, 789, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e/f"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e/f"))))

		// Removing all entries should make all instance names
		// unresolvable.
		require.False(t, it.Remove(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.False(t, it.Remove(util.Must(digest.NewInstanceName("a"))))
		require.True(t, it.Remove(util.Must(digest.NewInstanceName("a/b/c"))))

		require.Equal(t, -1, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e/f"))))
		require.False(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e/f"))))
	})

	t.Run("WithRoot", func(t *testing.T) {
		// Attempt to perform lookups on a trie that has a root node.
		it := digest.NewInstanceNameTrie()
		it.Set(digest.EmptyInstanceName, 123)
		it.Set(util.Must(digest.NewInstanceName("a/b")), 456)
		it.Set(util.Must(digest.NewInstanceName("a/b/c/d")), 789)

		require.Equal(t, 123, it.GetExact(digest.EmptyInstanceName))
		require.True(t, it.ContainsExact(digest.EmptyInstanceName))
		require.Equal(t, 123, it.GetLongestPrefix(digest.EmptyInstanceName))
		require.True(t, it.ContainsPrefix(digest.EmptyInstanceName))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("a"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("a"))))
		require.Equal(t, 123, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a"))))

		require.Equal(t, 456, it.GetExact(util.Must(digest.NewInstanceName("a/b"))))
		require.True(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b"))))
		require.Equal(t, 456, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b"))))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("a/b/c"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c"))))
		require.Equal(t, 456, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c"))))

		require.Equal(t, 789, it.GetExact(util.Must(digest.NewInstanceName("a/b/c/d"))))
		require.True(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c/d"))))
		require.Equal(t, 789, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d"))))

		require.Equal(t, -1, it.GetExact(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.False(t, it.ContainsExact(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.Equal(t, 789, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.True(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e"))))

		// Removing all entries should make all instance names
		// unresolvable.
		require.False(t, it.Remove(digest.EmptyInstanceName))
		require.False(t, it.Remove(util.Must(digest.NewInstanceName("a/b"))))
		require.True(t, it.Remove(util.Must(digest.NewInstanceName("a/b/c/d"))))

		require.Equal(t, -1, it.GetLongestPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
		require.False(t, it.ContainsPrefix(util.Must(digest.NewInstanceName("a/b/c/d/e"))))
	})
}
