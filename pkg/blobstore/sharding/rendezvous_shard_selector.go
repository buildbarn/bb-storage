package sharding

import (
	"hash/fnv"
	"math"
)

type rendezvousShardSelector struct {
	keyMap	map[uint64]string
	weightMap map[uint64]uint32
}

func hashServer(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func NewRendezvousShardSelector(weights map[string]uint32) *rendezvousShardSelector {
	weightMap := make(map[uint64]uint32, len(weights))
	keyMap := make(map[uint64]string, len(weights))

	for key, weight := range weights {
		keyHash := hashServer(key)
		keyMap[keyHash] = key
		weightMap[keyHash] = weight
	}
	return &rendezvousShardSelector{
		keyMap:	keyMap,
		weightMap: weightMap,
	}
}

func score(x uint64) float64 {
	// branchless clamp to [1,MAX_UINT64-1]
	x = x - ((x|-x)>>63) + (((^x)|-(^x)) >> 63)
	frac := float64(x)/float64(^uint64(0))
	return 1.0/-math.Log(frac)
}

// PRNG without branches or allocations
func splitmix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func (s *rendezvousShardSelector) GetShard(hash uint64) string {
	var best float64
	var bestKey string

	for keyHash, weight := range s.weightMap {
		mixed := splitmix64(hash^keyHash)
		current := float64(weight) * score(mixed)
		if current > best {
			best = current
			bestKey = s.keyMap[keyHash]
		}
	}
	return bestKey
}
