package random

import (
	"time"
)

// SingleThreadedGenerator is a Random Number Generator (RNG) that
// cannot be used concurrently. This interface is a subset of Go's
// rand.Rand.
type SingleThreadedGenerator interface {
	// Generates a number in range [0.0, 1.0).
	Float64() float64
	// Generates a number in range [0, n), where n is of type int64.
	Int64N(n int64) int64
	// Generates a number in range [0, n), where n is of type int.
	IntN(n int) int
	// Generates arbitrary bytes of data. This method is guaranteed
	// to succeed.
	Read(p []byte) (int, error)
	// Shuffle the elements in a list.
	Shuffle(n int, swap func(i, j int))
	// Generates an arbitrary 32-bit integer value.
	Uint32() uint32
	// Generates an arbitrary 64-bit integer value.
	Uint64() uint64
}

// Duration that is randomly generated that lies between [0, maximum).
func Duration(generator SingleThreadedGenerator, maximum time.Duration) time.Duration {
	return time.Duration(generator.Int64N(maximum.Nanoseconds())) * time.Nanosecond
}
