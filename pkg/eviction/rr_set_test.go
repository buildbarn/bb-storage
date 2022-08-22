package eviction_test

import (
	"sort"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/stretchr/testify/require"
)

func TestRRSetExample(t *testing.T) {
	set := eviction.NewRRSet[string]()

	// Insert a set of words.
	words := []string{
		"abele", "furfuraceous", "narial", "rugine",
		"terrazzo", "ultrafidian", "unicity", "xesturgy",
	}
	for _, word := range words {
		set.Insert(word)
	}

	// Touch some of them. This should have no effect, as Random
	// Replacement does not respect any order.
	set.Touch("furfuraceous")
	set.Touch("unicity")

	// Remove all of the words from the set. They should be returned
	// in the same order at which they were inserted. Test that only
	// peeking at them doesn't remove them.
	extractedWords := make([]string, 0, len(words))
	for i := 0; i < len(words); i++ {
		extractedWords = append(extractedWords, set.Peek())
		set.Remove()
	}
	sort.Strings(extractedWords)
	require.Equal(t, words, extractedWords)
}
