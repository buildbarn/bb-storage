package sharding

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"sort"
)

type rendezvousShard struct {
	key string
	weight uint32
	index int
	hash uint64
}

type rendezvousShardSelector struct {
	shards []rendezvousShard
}

func hashServer(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func hashLut() uint64 {
	h := fnv.New64a()
	buf := make([]byte, len(log2LUT)*2)
	for i, v := range log2LUT {
		binary.LittleEndian.PutUint16(buf[i*2:], v)
	}
	h.Write(buf)
	return h.Sum64()
}

func NewRendezvousShardSelector(shards []Shard) (*rendezvousShardSelector, error) {
	// verify integrity of the look up table
	if lut := hashLut(); lut != 0xb61c4caeda464106 {
		return nil, fmt.Errorf("Can not shard correctly, log lookup table is broken on this platform got 0x%x expected 0x%x", lut, uint64(0xb61c4caeda464106))
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("RendezvousShardSelector must have shards to be defined")
	}
	internalShards := make([]rendezvousShard, 0, len(shards))
	keyMap := make(map[uint64]string, len(shards))
	for index, shard := range shards {
		hash := hashServer(shard.Key)
		if collision, exists := keyMap[hash]; exists {
			return nil, fmt.Errorf("Hash collision between shards: %s and %s", shard.Key, collision)
		}
		keyMap[hash] = shard.Key
		internalShards = append(internalShards, rendezvousShard{
			key: shard.Key,
			index: index,
			weight: shard.Weight,
			hash: hash,
		})
	}
	sort.Slice(internalShards, func(i, j int) bool {
		return internalShards[i].hash < internalShards[j].hash
	})
	return &rendezvousShardSelector{ shards: internalShards }, nil
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
	//
	// We will make 0 exclusive by simply clamping the value to a minimum of 1.
	if x == 0 {
		x = 1
	}
	logFixed := uint64(64)<<LOG_BITS - Log2Fixed(x)
	// Replace weight with fixed point representation of weight. We're not using
	// floating point math so we relative size of the weight to be as big as
	// possible compared to the log. Since weight is 32 bit it is safe to shift
	// it by an additional 32 bits. The maximum number of bits of the log is
	// 5+LOG_BITS.
	weightFixed := uint64(weight)<<(64-MAX_WEIGHT_BITS)
	return weightFixed/logFixed
}

const LOG_BITS = 15
const LUT_ENTRY_BITS = 6
const MAX_WEIGHT_BITS = 32
// Lookup table used for the log2 fraction, it is a fixed point representation
// with LOG_BITS precision containing 1<<LUT_ENTRY_BITS entries. The entry is
// picked by truncating to the remaining LUT_ENTRY_BITS of precision. We add an
// additional value to help with interpolation.
//
// Values are guaranteed to be stable as long as we can rely on the inbuilt log2
// to give atleast LOG_BITS precision. We perform a hash of the lookup table at
// startup to verify that it has produced the expected values.
var log2LUT [(1<<LUT_ENTRY_BITS)+1]uint16

func init() {
	for i := 0; i < (1 << LUT_ENTRY_BITS); i++ {
		target := 1.0 + float64(i) / float64(1<<LUT_ENTRY_BITS)
		val := uint16(math.Round(math.Log2(target)*math.Pow(2.0, float64(LOG_BITS))))
		if val >= uint16(1) << LOG_BITS {
			val = (uint16(1)<<LOG_BITS) - 1
		}
		log2LUT[i] = val
	}
	log2LUT[1<<LUT_ENTRY_BITS] = uint16(1)<<LOG_BITS
}

func Log2Fixed(x uint64) uint64 {
	// Fixed point approximation of log2 with a lookup table for deterministic
	// math. Calculates the logarithm as the sum of three pieces:
	// 1. The integer value, which is calculated by counting number of bits.
	// 2. A value calculated by a lookup table of LUT_ENTRY_BITS
	// 3. The linearly interpolated value between the lookup table and the next value.
	//
	// log2(x) = N+log2(x/2^N) we pick the N integer part of the logaritm which
	// we can calculate exactly by counting the number of bits in the number.
	// x/2^N will then be a number between 1 and 2 for which we can use a lookup
	// table to get precomputed values.
	msb := bits.Len64(x)-1 
	var bitfield uint64
	if msb >= LOG_BITS {
		bitfield = (x >> (msb-LOG_BITS)) & ((1<<LOG_BITS)-1)
	} else {
		bitfield = (x << (LOG_BITS-msb)) & ((1<<LOG_BITS)-1)
	}
	// Use the first LUT_ENTRY_BITS to look up the index, use the remaining
	// fraction to partially interpolate to the next entry.
	INTERP_BITS := LOG_BITS-LUT_ENTRY_BITS
	index  := bitfield>>INTERP_BITS
	interp := bitfield&((1<<INTERP_BITS)-1)
	baseVal := uint64(log2LUT[index])
	nextVal := uint64(log2LUT[index+1])
	delta := nextVal-baseVal
	frac := baseVal + (delta * interp) >> INTERP_BITS
	return (uint64(msb) << LOG_BITS) | uint64(frac)
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

func (s *rendezvousShardSelector) GetShard(hash uint64) int {
	var best uint64
	var bestIndex int
	for _, shard := range s.shards {
		mixed := splitmix64(shard.hash^hash)
		current := score(mixed, shard.weight)
		if current > best {
			best = current
			bestIndex = shard.index
		}
	}
	return bestIndex
}
