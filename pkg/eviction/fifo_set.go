package eviction

type fifoSet[T any] struct {
	elements []T
}

// NewFIFOSet creates a new cache replacement set that implements the
// First In First Out (FIFO) policy.
//
// https://en.wikipedia.org/wiki/Cache_replacement_policies#First_in_first_out_(FIFO)
func NewFIFOSet[T any]() Set[T] {
	return &fifoSet[T]{}
}

func (s *fifoSet[T]) Insert(value T) {
	s.elements = append(s.elements, value)
}

func (fifoSet[T]) Touch(value T) {
}

func (s *fifoSet[T]) Peek() T {
	return s.elements[0]
}

func (s *fifoSet[T]) Remove() {
	s.elements = s.elements[1:]
}
