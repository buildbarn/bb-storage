package random

import (
	"math/rand"
)

// SingleThreadedGenerator is a Random Number Generator (RNG) that
// cannot be used concurrently. This interface is a subset of Go's
// rand.Rand.
type SingleThreadedGenerator interface {
	// Generates a number in range [0.0, 1.0).
	Float64() float64
	// Generates a number in range [0, n), where n is of type int64.
	Int63n(n int64) int64
	// Generates a number in range [0, n), where n is of type int.
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
