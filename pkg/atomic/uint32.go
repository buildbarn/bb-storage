package atomic

import (
	"sync/atomic"
)

// Uint32 holds a uint32 that can only be accessed through atomic
// operations. This type is guaranteed to be properly aligned. Instances
// of this type cannot be moved to a different location in memory.
type Uint32 struct {
	v uint32
}

// Add a value atomically, similar to atomic.AddUint32().
func (i *Uint32) Add(delta uint32) uint32 {
	return atomic.AddUint32(&i.v, delta)
}

// CompareAndSwap executes a compare-and-swap, similar to
// atomic.CompareAndSwapUint32().
func (i *Uint32) CompareAndSwap(old, new uint32) bool {
	return atomic.CompareAndSwapUint32(&i.v, old, new)
}

// Initialize the atomic variable with a given value. The atomic
// variable will be zero initialized when not called.
func (i *Uint32) Initialize(val uint32) {
	i.v = val
}

// Load a value atomically, similar to atomic.LoadUint32().
func (i *Uint32) Load() uint32 {
	return atomic.LoadUint32(&i.v)
}

// Store a value atomically, similar to atomic.StoreUint32().
func (i *Uint32) Store(val uint32) {
	atomic.StoreUint32(&i.v, val)
}
