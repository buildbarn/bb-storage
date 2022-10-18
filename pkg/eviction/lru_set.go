package eviction

type lruSet[T comparable] struct {
	// Doubly linked list for storing elements in eviction order.
	head lruElement[T]

	// Map for looking up elements to be able to touch them.
	elements map[T]*lruElement[T]
}

// NewLRUSet creates a new cache replacement set that implements the
// Least Recently Used (LRU) policy.
//
// https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)
func NewLRUSet[T comparable]() Set[T] {
	s := &lruSet[T]{
		elements: map[T]*lruElement[T]{},
	}
	s.head.older = &s.head
	s.head.newer = &s.head
	return s
}

func (s *lruSet[T]) insertIntoQueue(e *lruElement[T]) {
	e.older = s.head.older
	e.newer = &s.head
	e.older.newer = e
	e.newer.older = e
}

func (s *lruSet[T]) Insert(value T) {
	if _, ok := s.elements[value]; ok {
		panic("Attempted to insert value into cache replacement set twice")
	}
	e := &lruElement[T]{value: value}
	s.insertIntoQueue(e)
	s.elements[value] = e
}

func (s *lruSet[T]) Touch(value T) {
	e := s.elements[value]
	e.removeFromQueue()
	s.insertIntoQueue(e)
}

func (s *lruSet[T]) Peek() T {
	return s.head.newer.value
}

func (s *lruSet[T]) Remove() {
	e := s.head.newer
	e.removeFromQueue()
	delete(s.elements, e.value)
}

type lruElement[T any] struct {
	older *lruElement[T]
	newer *lruElement[T]
	value T
}

func (e *lruElement[T]) removeFromQueue() {
	e.older.newer = e.newer
	e.newer.older = e.older
	e.older = nil
	e.newer = nil
}
