package random

import (
	"sync"
)

type fastThreadSafeGenerator struct {
	lock      sync.Mutex
	generator SingleThreadedGenerator
}

func (g *fastThreadSafeGenerator) IsThreadSafe() {}

func (g *fastThreadSafeGenerator) Float64() float64 {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.generator.Float64()
}

func (g *fastThreadSafeGenerator) Int63n(n int64) int64 {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.generator.Int63n(n)
}

func (g *fastThreadSafeGenerator) Intn(n int) int {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.generator.Intn(n)
}

func (g *fastThreadSafeGenerator) Read(p []byte) (int, error) {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.generator.Read(p)
}

func (g *fastThreadSafeGenerator) Shuffle(n int, swap func(i, j int)) {
	g.lock.Lock()
	defer g.lock.Unlock()
	g.generator.Shuffle(n, swap)
}

func (g *fastThreadSafeGenerator) Uint64() uint64 {
	g.lock.Lock()
	defer g.lock.Unlock()
	return g.generator.Uint64()
}

// FastThreadSafeGenerator is an instance of ThreadSafeGenerator that is
// not suitable for cryptographic purposes. The generator is randomly
// seeded on startup.
var FastThreadSafeGenerator ThreadSafeGenerator = &fastThreadSafeGenerator{
	generator: NewFastSingleThreadedGenerator(),
}
