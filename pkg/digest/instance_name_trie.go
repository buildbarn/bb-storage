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

// Set an instance name in the trie to a given integer value.
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

// ContainsExact returns whether the trie contains an instance name that
// is exactly the same as the one provided.
func (it *InstanceNameTrie) ContainsExact(i InstanceName) bool {
	return it.GetExact(i) >= 0
}

// ContainsPrefix returns whether the trie contains one or more instance
// names that are a prefix of the one provided.
func (it *InstanceNameTrie) ContainsPrefix(i InstanceName) bool {
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
			nFinal, ok := n.children[in]
			return ok && nFinal.value >= 0
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

// GetExact returns the value associated with the instance name. If none
// of the instance names provided to Set() are exactly the same as the
// instance name provided to GetExact(), this function returns -1.
func (it *InstanceNameTrie) GetExact(i InstanceName) int {
	// Special case: empty instance name.
	in := i.String()
	if in == "" {
		return it.root.value
	}

	n := &it.root
	for {
		idx := strings.IndexByte(in, '/')
		if idx < 0 {
			// Last component in the instance name.
			if nFinal, ok := n.children[in]; ok && nFinal.value >= 0 {
				return nFinal.value
			}
			return -1
		}

		// More components follow.
		nNext, ok := n.children[in[:idx]]
		if !ok {
			return -1
		}
		n = nNext
		in = in[idx+1:]
	}
}

// GetLongestPrefix returns the value associated with the longest
// matching instance name prefix. If none of the instance names provided
// to Set() are a prefix of the instance name provided to
// GetLongestPrefix(), this function returns -1.
func (it *InstanceNameTrie) GetLongestPrefix(i InstanceName) int {
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

// Remove a value associated with an instance name. This function
// returns whether removing the instance name has caused the trie to
// become empty.
func (it *InstanceNameTrie) Remove(i InstanceName) bool {
	// Special case: empty instance name.
	in := i.String()
	if in == "" {
		it.root.value = -1
		return len(it.root.children) == 0
	}

	n := &it.root
	var mapDelete map[string]*instanceNameTrieNode
	var componentDelete string
	for {
		// Extract the first component of the instance name.
		var component string
		idx := strings.IndexByte(in, '/')
		if idx < 0 {
			component = in
		} else {
			component = in[:idx]
		}
		in = in[idx+1:]

		// Capture edge in the trie that can be cut in case
		// there are no other entries below.
		if n.value >= 0 || len(n.children) > 1 || mapDelete == nil {
			mapDelete = n.children
			componentDelete = component
		}

		n = n.children[component]
		if idx < 0 {
			// Last component in the instance name.
			if len(n.children) == 0 {
				// No further children underneath.
				// Cut off a part of the trie.
				delete(mapDelete, componentDelete)
			} else {
				// More children underneath. We cannot
				// cut off this part of the trie.
				n.value = -1
			}
			return it.root.value < 0 && len(it.root.children) == 0
		}
	}
}

// InstanceNameMatcher is a function callback type that corresponds with
// the signature of InstanceNameTrie.ContainsPrefix. It can be used in
// places where an InstanceNameTrie is used as a simple set.
type InstanceNameMatcher func(i InstanceName) bool

var (
	_ InstanceNameMatcher = (*InstanceNameTrie)(nil).ContainsExact
	_ InstanceNameMatcher = (*InstanceNameTrie)(nil).ContainsPrefix
)
