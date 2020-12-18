package atomic

import (
	"sync/atomic"
)

// Int32 holds an int32 that can only be accessed through atomic
// operations. This type is guaranteed to be properly aligned. Instances
// of this type cannot be moved to a different location in memory.
type Int32 struct {
	v int32
}

// Add a value atomically, similar to atomic.AddInt32().
func (i *Int32) Add(delta int32) int32 {
	return atomic.AddInt32(&i.v, delta)
}

// CompareAndSwap executes a compare-and-swap, similar to
// atomic.CompareAndSwapInt32().
func (i *Int32) CompareAndSwap(old, new int32) bool {
	return atomic.CompareAndSwapInt32(&i.v, old, new)
}

// Initialize the atomic variable with a given value. The atomic
// variable will be zero initialized when not called.
func (i *Int32) Initialize(val int32) {
	i.v = val
}

// Load a value atomically, similar to atomic.LoadInt32().
func (i *Int32) Load() int32 {
	return atomic.LoadInt32(&i.v)
}

// Store a value atomically, similar to atomic.StoreInt32().
func (i *Int32) Store(val int32) {
	atomic.StoreInt32(&i.v, val)
}
