package eviction_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/stretchr/testify/require"
)

func TestFIFOSetExample(t *testing.T) {
	set := eviction.NewFIFOSet[string]()

	// Insert a set of words.
	words := []string{
		"woggle", "aulete", "zoophysiology", "stepney",
		"zizyphus", "melologue", "heortology", "owling",
	}
	for _, word := range words {
		set.Insert(word)
	}

	// Touch some of them. This should have no effect, as First In
	// First Out only respects insertion order.
	set.Touch("zoophysiology")
	set.Touch("melologue")

	// Remove all of the words from the set. They should be returned
	// in the same order at which they were inserted. Test that only
	// peeking at them doesn't remove them.
	for _, word := range words {
		require.Equal(t, word, set.Peek())
		require.Equal(t, word, set.Peek())
		set.Remove()
	}
}
