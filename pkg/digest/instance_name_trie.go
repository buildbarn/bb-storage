package digest

import (
	"strings"
)

type instanceNameTrieNode struct {
	children map[string]*instanceNameTrieNode
	value    int
}

// InstanceNameTrie implements a trie (prefix tree) for instance names.
// It can be used to demultiplex remote execution requests based on the
// instance name.
//
// For every key stored in the trie, an integer value is tracked. This
// can, for example, be used by the caller to look up a corresponding
// value in a contiguous list.
type InstanceNameTrie struct {
	root instanceNameTrieNode
}

// NewInstanceNameTrie creates a new InstanceNameTrie that is
// initialized with no elements.
func NewInstanceNameTrie() *InstanceNameTrie {
	// Create an empty trie.
	it := &InstanceNameTrie{
		root: instanceNameTrieNode{
			children: map[string]*instanceNameTrieNode{},
			value:    -1,
		},
	}

	return it
}

// Set an instance name prefix in the trie to a given integer value.
func (it *InstanceNameTrie) Set(i InstanceName, value int) {
	// Insert provided instance names into the trie.
	components := strings.FieldsFunc(i.value, func(r rune) bool { return r == '/' })
	n := &it.root
	for _, component := range components {
		nNext, ok := n.children[component]
		if !ok {
			nNext = &instanceNameTrieNode{
				children: map[string]*instanceNameTrieNode{},
				value:    -1,
			}
			n.children[component] = nNext
		}
		n = nNext
	}
	n.value = value
}

// Contains returns whether the trie contains one or more instance name
// prefixes that are a prefix of the provided instance name.
func (it *InstanceNameTrie) Contains(i InstanceName) bool {
	// Special case: empty instance name.
	if it.root.value >= 0 {
		return true
	}
	in := i.String()
	if in == "" {
		return false
	}

	n := &it.root
	for {
		idx := strings.IndexByte(in, '/')
		if idx < 0 {
			// Last component in the instance name.
			if nFinal, ok := n.children[in]; ok && nFinal.value >= 0 {
				return true
			}
			return false
		}

		// More components follow.
		nNext, ok := n.children[in[:idx]]
		if !ok {
			return false
		}
		n = nNext
		in = in[idx+1:]
		if n.value >= 0 {
			return true
		}
	}
}

// Get value associated with the longest matching instance name prefix.
// If none of the instance names provided to Set() are a prefix of the
// instance name provided to Get(), this function returns -1.
func (it *InstanceNameTrie) Get(i InstanceName) int {
	// Special case: empty instance name.
	in := i.String()
	if in == "" {
		return it.root.value
	}

	lastValue := it.root.value
	n := &it.root
	for {
		idx := strings.IndexByte(in, '/')
		if idx < 0 {
			// Last component in the instance name.
			if nFinal, ok := n.children[in]; ok && nFinal.value >= 0 {
				return nFinal.value
			}
			return lastValue
		}

		// More components follow.
		nNext, ok := n.children[in[:idx]]
		if !ok {
			return lastValue
		}
		n = nNext
		in = in[idx+1:]
		if n.value >= 0 {
			// New longest prefix match.
			lastValue = n.value
		}
	}
}

// InstanceNameMatcher is a function callback type that corresponds with
// the signature of InstanceNameTrie.Contains. It can be used in places
// where an InstanceNameTrie is used as a simple set.
type InstanceNameMatcher func(i InstanceName) bool

var _ InstanceNameMatcher = (*InstanceNameTrie)(nil).Contains
