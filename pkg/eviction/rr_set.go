package eviction

import (
	"math/rand"
)

type rrSet struct {
	elements []string
}

// NewRRSet creates a new cache replacement set that implements the
// Random Replacement (RR) policy.
//
// https://en.wikipedia.org/wiki/Cache_replacement_policies#Random_replacement_(RR)
func NewRRSet() Set {
	return &rrSet{}
}

func (s *rrSet) Insert(value string) {
	// Insert element into a random location in the list, opening up
	// space by moving an existing element to the end of the list.
	index := rand.Intn(len(s.elements) + 1)
	if index == len(s.elements) {
		s.elements = append(s.elements, value)
	} else {
		s.elements = append(s.elements, s.elements[index])
		s.elements[index] = value
	}
}

func (s *rrSet) Touch(value string) {
}

func (s *rrSet) Peek() string {
	return s.elements[len(s.elements)-1]
}

func (s *rrSet) Remove() {
	s.elements = s.elements[:len(s.elements)-1]
}
