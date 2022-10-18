package eviction_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/stretchr/testify/require"
)

func TestLRUSetExample(t *testing.T) {
	set := eviction.NewLRUSet[string]()

	// Insert a set of words.
	words := []string{
		"gemmation", "jordan", "villose", "zoogeography",
		"goa", "torfaceous", "xanthochroia", "grattoir",
	}
	for _, word := range words {
		set.Insert(word)
	}

	// Touch some of them. This should cause these entries to be
	// returned last.
	set.Touch("xanthochroia")
	set.Touch("gemmation")

	// Remove all of the words from the set. They should be returned
	// in the same order at which they were inserted or touched.
	// Test that only peeking at them doesn't remove them.
	extractedWords := []string{
		"jordan", "villose", "zoogeography", "goa",
		"torfaceous", "grattoir", "xanthochroia", "gemmation",
	}
	for _, word := range extractedWords {
		require.Equal(t, word, set.Peek())
		require.Equal(t, word, set.Peek())
		set.Remove()
	}
}
