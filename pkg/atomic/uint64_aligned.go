// +build amd64 arm64

package atomic

// Uint64 holds a uint64 that can only be accessed through atomic
// operations. This type is guaranteed to be properly aligned. Instances
// of this type cannot be moved to a different location in memory.
type Uint64 struct {
	v uint64
}

func (i *Uint64) get() *uint64 {
	return &i.v
}
