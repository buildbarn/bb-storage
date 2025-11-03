package random

import (
	crypto_rand "crypto/rand"
	"encoding/binary"
	"fmt"
	math_rand "math/rand/v2"
)

func mustCryptoRandRead(p []byte) (int, error) {
	n, err := crypto_rand.Read(p)
	if err != nil {
		panic(fmt.Sprintf("Failed to obtain random data: %s", err))
	}
	return n, nil
}

type cryptoSource struct{}

func (cryptoSource) Uint64() uint64 {
	var b [8]byte
	mustCryptoRandRead(b[:])
	return binary.LittleEndian.Uint64(b[:])
}

var _ math_rand.Source = cryptoSource{}

type cryptoThreadSafeGenerator struct {
	*math_rand.Rand
}

func (cryptoThreadSafeGenerator) IsThreadSafe() {}

func (cryptoThreadSafeGenerator) Read(p []byte) (int, error) {
	return mustCryptoRandRead(p)
}

// CryptoThreadSafeGenerator is an instance of ThreadSafeGenerator that is
// suitable for cryptographic purposes.
var CryptoThreadSafeGenerator ThreadSafeGenerator = cryptoThreadSafeGenerator{
	Rand: math_rand.New(cryptoSource{}),
}
