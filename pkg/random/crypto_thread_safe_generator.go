package random

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	math_rand "math/rand"
)

func mustCryptoRandRead(p []byte) (int, error) {
	n, err := crypto_rand.Read(p)
	if err != nil {
		panic(fmt.Sprintf("Failed to obtain random data: %s", err))
	}
	return n, nil
}

type cryptoSource64 struct{}

func (s cryptoSource64) Int63() int64 {
	return int64(s.Uint64() >> 1)
}

func (s cryptoSource64) Uint64() uint64 {
	var b [8]byte
	mustCryptoRandRead(b[:])
	return binary.LittleEndian.Uint64(b[:])
}

func (s cryptoSource64) Seed(seed int64) {
	panic("Crypto source cannot be seeded")
}

var _ math_rand.Source64 = cryptoSource64{}

type cryptoThreadSafeGenerator struct {
	*math_rand.Rand
}

func (g cryptoThreadSafeGenerator) IsThreadSafe() {}

func (g cryptoThreadSafeGenerator) Read(p []byte) (int, error) {
	// Call into crypto_rand.Read() directly, as opposed to using
	// math_rand.Rand.Read().
	return mustCryptoRandRead(p)
}

// CryptoThreadSafeGenerator is an instance of ThreadSafeGenerator that is
// suitable for cryptographic purposes.
var CryptoThreadSafeGenerator ThreadSafeGenerator = cryptoThreadSafeGenerator{
	Rand: math_rand.New(cryptoSource64{}),
}
