package eviction

type lruSet struct {
	// Doubly linked list for storing elements in eviction order.
	head lruElement

	// Map for looking up elements to be able to touch them.
	elements map[string]*lruElement
}

// NewLRUSet creates a new cache replacement set that implements the
// Least Recently Used (LRU) policy.
//
// https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)
func NewLRUSet() Set {
	s := &lruSet{
		elements: map[string]*lruElement{},
	}
	s.head.older = &s.head
	s.head.newer = &s.head
	return s
}

func (s *lruSet) insertIntoQueue(e *lruElement) {
	e.older = s.head.older
	e.newer = &s.head
	e.older.newer = e
	e.newer.older = e
}

func (s *lruSet) Insert(value string) {
	if _, ok := s.elements[value]; ok {
		panic("Attempted to insert value into cache replacement set twice")
	}
	e := &lruElement{value: value}
	s.insertIntoQueue(e)
	s.elements[value] = e
}

func (s *lruSet) Touch(value string) {
	e := s.elements[value]
	e.removeFromQueue()
	s.insertIntoQueue(e)
}

func (s *lruSet) Peek() string {
	return s.head.newer.value
}

func (s *lruSet) Remove() {
	e := s.head.newer
	e.removeFromQueue()
	delete(s.elements, e.value)
}

type lruElement struct {
	older *lruElement
	newer *lruElement
	value string
}

func (e *lruElement) removeFromQueue() {
	e.older.newer = e.newer
	e.newer.older = e.older
	e.older = nil
	e.newer = nil
}
