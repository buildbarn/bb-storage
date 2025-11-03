package eviction

import (
	"github.com/buildbarn/bb-storage/pkg/random"
)

type rrSet[T any] struct {
	generator random.SingleThreadedGenerator
	elements  []T
}

// NewRRSet creates a new cache replacement set that implements the
// Random Replacement (RR) policy.
//
// https://en.wikipedia.org/wiki/Cache_replacement_policies#Random_replacement_(RR)
func NewRRSet[T any]() Set[T] {
	return &rrSet[T]{
		generator: random.NewFastSingleThreadedGenerator(),
	}
}

func (s *rrSet[T]) Insert(value T) {
	// Insert element into a random location in the list, opening up
	// space by moving an existing element to the end of the list.
	index := s.generator.IntN(len(s.elements) + 1)
	if index == len(s.elements) {
		s.elements = append(s.elements, value)
	} else {
		s.elements = append(s.elements, s.elements[index])
		s.elements[index] = value
	}
}

func (rrSet[T]) Touch(value T) {
}

func (s *rrSet[T]) Peek() T {
	return s.elements[len(s.elements)-1]
}

func (s *rrSet[T]) Remove() {
	s.elements = s.elements[:len(s.elements)-1]
}
