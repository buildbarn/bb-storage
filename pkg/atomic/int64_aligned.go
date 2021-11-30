//go:build amd64 || arm64
// +build amd64 arm64

package atomic

// Int64 holds an int64 that can only be accessed through atomic
// operations. This type is guaranteed to be properly aligned. Instances
// of this type cannot be moved to a different location in memory.
type Int64 struct {
	v int64
}

func (i *Int64) get() *int64 {
	return &i.v
}
