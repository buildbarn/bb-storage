package random_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/random"
	"github.com/stretchr/testify/require"
)

func TestSingleThreadedGenerator(t *testing.T) {
	for name, generator := range map[string]random.SingleThreadedGenerator{
		"FastSingleThreaded": random.NewFastSingleThreadedGenerator(),
		"FastThreadSafe":     random.FastThreadSafeGenerator,
		"CryptoThreadSafe":   random.CryptoThreadSafeGenerator,
	} {
		t.Run(name, func(t *testing.T) {
			t.Run("Intn", func(t *testing.T) {
				for i := 0; i < 100; i++ {
					v := generator.Intn(42)
					require.LessOrEqual(t, 0, v)
					require.Greater(t, 42, v)
				}
			})

			t.Run("Read", func(t *testing.T) {
				var b [8]byte
				generator.Read(b[:])
			})

			t.Run("Shuffle", func(t *testing.T) {
				called := false
				for !called {
					generator.Shuffle(100, func(i, j int) {
						called = true
					})
				}
			})

			t.Run("Uint64", func(t *testing.T) {
				generator.Uint64()
			})
		})
	}
}
