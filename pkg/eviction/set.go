package eviction

// Set of values that need to be retained according to a cache
// replacement policy. This set does not permit concurrent access.
type Set[T any] interface {
	// Insert a value into the set. The value may not already be
	// present within the set.
	Insert(value T)

	// Touch the element stored in the set corresponding with the
	// provided value. Touching is used to indicate that the object
	// corresponding with the value was recently used.
	//
	// For cache replacement policies such as Least Recently Used
	// (LRU), this function causes the element in the set to be
	// moved to the back of a queue. For cache replacement policies
	// such as Random Replacement (RR), this function has no effect.
	//
	// The value must already be present within the set.
	Touch(value T)

	// Peek at the element that needs to be removed from cache
	// first. This function may not be called on empty sets.
	Peek() T

	// Remove the element from the set that was last returned by
	// Peek().
	Remove()
}
