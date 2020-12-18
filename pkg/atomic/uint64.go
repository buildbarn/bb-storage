package atomic

import (
	"sync/atomic"
)

// Add a value atomically, similar to atomic.AddUint64().
func (i *Uint64) Add(delta uint64) uint64 {
	return atomic.AddUint64(i.get(), delta)
}

// CompareAndSwap executes a compare-and-swap, similar to
// atomic.CompareAndSwapUint64().
func (i *Uint64) CompareAndSwap(old, new uint64) bool {
	return atomic.CompareAndSwapUint64(i.get(), old, new)
}

// Initialize the atomic variable with a given value. The atomic
// variable will be zero initialized when not called.
func (i *Uint64) Initialize(val uint64) {
	*i.get() = val
}

// Load a value atomically, similar to atomic.LoadUint64().
func (i *Uint64) Load() uint64 {
	return atomic.LoadUint64(i.get())
}

// Store a value atomically, similar to atomic.StoreUint64().
func (i *Uint64) Store(val uint64) {
	atomic.StoreUint64(i.get(), val)
}
