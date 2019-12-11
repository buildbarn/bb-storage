package eviction

type fifoSet struct {
	elements []string
}

// NewFIFOSet creates a new cache replacement set that implements the
// First In First Out (FIFO) policy.
//
// https://en.wikipedia.org/wiki/Cache_replacement_policies#First_in_first_out_(FIFO)
func NewFIFOSet() Set {
	return &fifoSet{}
}

func (s *fifoSet) Insert(value string) {
	s.elements = append(s.elements, value)
}

func (s *fifoSet) Touch(value string) {
}

func (s *fifoSet) Peek() string {
	return s.elements[0]
}

func (s *fifoSet) Remove() {
	s.elements = s.elements[1:]
}
