package random

import (
	"math/rand"
)

// SingleThreadedGenerator is a Random Number Generator (RNG) that
// cannot be used concurrently. This interface is a subset of Go's
// rand.Rand.
type SingleThreadedGenerator interface {
	// Generates a number in range [0, n).
	Intn(n int) int
	// Generates arbitrary bytes of data. This method is guaranteed
	// to succeed.
	Read(p []byte) (int, error)
	// Shuffle the elements in a list.
	Shuffle(n int, swap func(i, j int))
	// Generates an arbitrary 64-bit integer value.
	Uint64() uint64
}

var _ SingleThreadedGenerator = (*rand.Rand)(nil)
