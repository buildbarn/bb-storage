package sharding

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/bits"
	"sort"
)

type rendezvousShard struct {
	weight uint32
	index  int
	hash   uint64
}

type rendezvousShardSelector struct {
	shards []rendezvousShard
}

func hashServer(key string) uint64 {
	h := sha256.Sum256([]byte(key))
	return binary.BigEndian.Uint64(h[:8])
}

// NewRendezvousShardSelector performs shard selection using the Rendezvous
// Hashing algorithm. The algorithm distributes blobs over the shard
// proportional to the shards weight, it fullfils all required properties of the
// ShardSelector interface:
//   - Reordering the shards will not affect the chosen shard.
//   - Removing a shard is guaranteed to only affect blobs that would have
//     resolved to the removed shard.
//   - Adding a shard will only affect that blobs resolve to the new shard.
func NewRendezvousShardSelector(shards []Shard) (ShardSelector, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("RendezvousShardSelector must have shards to be defined")
	}
	internalShards := make([]rendezvousShard, 0, len(shards))
	keyMap := make(map[uint64]string, len(shards))
	for index, shard := range shards {
		hash := hashServer(shard.Key)
		if collision, exists := keyMap[hash]; exists {
			return nil, fmt.Errorf("hash collision between shards: %s and %s", shard.Key, collision)
		}
		keyMap[hash] = shard.Key
		internalShards = append(internalShards, rendezvousShard{
			index:  index,
			weight: shard.Weight,
			hash:   hash,
		})
	}
	sort.Slice(internalShards, func(i, j int) bool {
		return internalShards[i].hash < internalShards[j].hash
	})
	return &rendezvousShardSelector{shards: internalShards}, nil
}

func score(x uint64, weight uint32) uint64 {
	// The mathematical formula we are approximating is -weight/log(X) where X
	// is a uniform random number between ]0,1[. For stability and performance
	// reasons we are foregoing any floating point operations and approximating
	// the logarithm.
	//
	// Since we are interested in the relative ordering rather than the absolute
	// value of the score we can pick log2 as our desired implementation. Log2
	// is simple to approximate numerically.
	//
	// x is already random and uniform, we can turn it into a number between 0
	// (inclusive) and 1 (exclusive) by simply dividing by MaxUint64+1. By the
	// properties of the logarithm we can simplify -log2(x/(MaxUint64+1)) to
	// log2(MaxUint64+1)-log2(x), which will be 64-log2(x)
	logFixed := uint64(64)<<16 - Log2Fixed(x)
	// Replace weight with fixed point representation of weight. We're not using
	// floating point math so we relative size of the weight to be as big as
	// possible compared to the log. Since weight is 32 bit it is safe to shift
	// it by an additional 32 bits.
	weightFixed := uint64(weight) << 32
	return weightFixed / logFixed
}

const (
	lutEntryBits = 6
)

// Lookup table used for the log2 fraction, it is a fixed point representation
// of log2(x) for x between [1,2] which is a a value between 0 and 1. It uses 16
// bits of precision containing 1<<lutEntryBits+1 entries. The entry is picked
// by truncating to the remaining lutEntryBits of precision. We add the last
// value to simplify interpolation logic.
var lut = [(1 << lutEntryBits) + 1]uint16{
	0x0000, 0x05ba, 0x0b5d, 0x10eb, 0x1664, 0x1bc8, 0x2119, 0x2656,
	0x2b80, 0x3098, 0x359f, 0x3a94, 0x3f78, 0x444c, 0x4910, 0x4dc5,
	0x526a, 0x5700, 0x5b89, 0x6003, 0x646f, 0x68ce, 0x6d20, 0x7165,
	0x759d, 0x79ca, 0x7dea, 0x81ff, 0x8608, 0x8a06, 0x8dfa, 0x91e2,
	0x95c0, 0x9994, 0x9d5e, 0xa11e, 0xa4d4, 0xa881, 0xac24, 0xafbe,
	0xb350, 0xb6d9, 0xba59, 0xbdd1, 0xc140, 0xc4a8, 0xc807, 0xcb5f,
	0xceaf, 0xd1f7, 0xd538, 0xd872, 0xdba5, 0xded0, 0xe1f5, 0xe513,
	0xe82a, 0xeb3b, 0xee45, 0xf149, 0xf446, 0xf73e, 0xfa2f, 0xfd1a,
	0x0000, // the overflow of 0x10000, cancels out when interpolating
}

// Log2Fixed is a fixed point approximation of log2 with a lookup table for
// deterministic math. 16 bits of precision represents the fractional value.
// Calculates the logarithm as the sum of three pieces:
//
// 1. The integer value, which is calculated by counting number of bits.
//
// 2. A value calculated by a lookup table of lutEntryBits
//
// 3. The linearly interpolated value between the lookup table and the next
// value.
//
// Since log2(x) = N+log2(x/2^N) we can easily remove the integer part of the
// logaritm. We calculate that exactly by counting the number of bits in the
// number. log(x/2^N) will then be a number between 0 and 1 for which we can use
// a lookup table to get precomputed values.
//
// In contrast with mathematical logarithm, this function is defined for x=0
// removing the need for conditionals the maximum value this function produces
// is 64 << 16 - 1.
func Log2Fixed(x uint64) uint64 {
	msb := bits.Len64(x >> 1)
	bitfield := x << (64 - msb)
	index := bitfield >> (64 - lutEntryBits)
	interp := bitfield << lutEntryBits >> 16
	base := lut[index]
	next := lut[index+1]
	delta := uint64(next - base)
	frac := uint64(base)<<48 + (delta * interp)
	return (uint64(msb) << 16) | uint64(frac)>>48
}

// A very fast PRNG with strong mixing properties
func splitmix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func (s *rendezvousShardSelector) GetShard(hash uint64) int {
	var best uint64
	var bestIndex int
	for _, shard := range s.shards {
		mixed := splitmix64(shard.hash ^ hash)
		current := score(mixed, shard.weight)
		if current > best {
			best = current
			bestIndex = shard.index
		}
	}
	return bestIndex
}
