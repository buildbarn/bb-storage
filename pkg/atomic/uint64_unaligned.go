// +build 386 arm

package atomic

import (
	"unsafe"
)

// Uint64 holds a uint64 that can only be accessed through atomic
// operations. This type is guaranteed to be properly aligned. Instances
// of this type cannot be moved to a different location in memory.
type Uint64 struct {
	v [3]uint32
}

func (i *Uint64) get() *uint64 {
	// This platform aligns 64-bit values at 4-byte offsets. Solve
	// this by allocating 12 bytes and rounding up the pointer value
	// to be 8-byte aligned.
	if p := unsafe.Pointer(&i.v[0]); uintptr(p)%8 == 0 {
		return (*uint64)(p)
	}
	return (*uint64)(unsafe.Pointer(&i.v[1]))
}
