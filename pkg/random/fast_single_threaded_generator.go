package random

import (
	"math/rand"
)

// NewFastSingleThreadedGenerator creates a new SingleThreadedGenerator
// that is not suitable for cryptographic purposes. The generator is
// randomly seeded.
func NewFastSingleThreadedGenerator() SingleThreadedGenerator {
	return rand.New(
		rand.NewSource(
			int64(CryptoThreadSafeGenerator.Uint64())))
}
