package random

import (
	"math/rand/v2"
)

type fastThreadSafeGenerator struct{}

func (fastThreadSafeGenerator) IsThreadSafe() {}

func (fastThreadSafeGenerator) Float64() float64 {
	return rand.Float64()
}

func (fastThreadSafeGenerator) Int64N(n int64) int64 {
	return rand.Int64N(n)
}

func (fastThreadSafeGenerator) IntN(n int) int {
	return rand.IntN(n)
}

func (fastThreadSafeGenerator) Read(p []byte) (int, error) {
	return mustCryptoRandRead(p)
}

func (fastThreadSafeGenerator) Shuffle(n int, swap func(i, j int)) {
	rand.Shuffle(n, swap)
}

func (fastThreadSafeGenerator) Uint32() uint32 {
	return rand.Uint32()
}

func (fastThreadSafeGenerator) Uint64() uint64 {
	return rand.Uint64()
}

// FastThreadSafeGenerator is an instance of ThreadSafeGenerator that is
// not suitable for cryptographic purposes. The generator is randomly
// seeded on startup.
var FastThreadSafeGenerator ThreadSafeGenerator = fastThreadSafeGenerator{}
