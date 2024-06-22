package random

import (
	"math/rand/v2"
)

type fastSingleThreadedGenerator struct {
	*rand.Rand
}

// NewFastSingleThreadedGenerator creates a new SingleThreadedGenerator
// that is not suitable for cryptographic purposes. The generator is
// randomly seeded.
func NewFastSingleThreadedGenerator() SingleThreadedGenerator {
	return fastSingleThreadedGenerator{
		Rand: rand.New(
			rand.NewPCG(
				CryptoThreadSafeGenerator.Uint64(),
				CryptoThreadSafeGenerator.Uint64(),
			),
		),
	}
}

func (fastSingleThreadedGenerator) Read(p []byte) (int, error) {
	return mustCryptoRandRead(p)
}
