package atomic

import (
	"sync/atomic"
)

// Add a value atomically, similar to atomic.AddInt64().
func (i *Int64) Add(delta int64) int64 {
	return atomic.AddInt64(i.get(), delta)
}

// CompareAndSwap executes a compare-and-swap, similar to
// atomic.CompareAndSwapInt64().
func (i *Int64) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64(i.get(), old, new)
}

// Initialize the atomic variable with a given value. The atomic
// variable will be zero initialized when not called.
func (i *Int64) Initialize(val int64) {
	*i.get() = val
}

// Load a value atomically, similar to atomic.LoadInt64().
func (i *Int64) Load() int64 {
	return atomic.LoadInt64(i.get())
}

// Store a value atomically, similar to atomic.StoreInt64().
func (i *Int64) Store(val int64) {
	atomic.StoreInt64(i.get(), val)
}
