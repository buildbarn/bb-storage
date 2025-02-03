package sharding_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/stretchr/testify/require"
)

func TestLog2Fixed(t *testing.T) {
	bits := sharding.LOG_BITS
	// test all powers of 2 (answer should be exact)
	for i := 0; i < 64; i++ {
		expected := uint64(i)<<bits
		actual := sharding.Log2Fixed(uint64(1)<<i)
		require.Equal(t, expected, actual, "Power of two should give exact result")
	}
	// test numbers < 100_000, expect less than 0.01% relative error from true result
	for i := 2; i < 100_000; i++ {
		expected := math.Log2(float64(i))
		actual := float64(sharding.Log2Fixed(uint64(i))) / math.Pow(2,float64(bits))
		require.InEpsilon(t, expected, actual, 1e-4, fmt.Sprintf("Error is too high for %d", i))
	}
}


const COUNT = 10_000_000
var precomputedResults = [20]int{3, 3, 1, 3, 4, 4, 1, 3, 2, 3, 0, 3, 4, 2, 4, 1, 3, 0, 3, 2}
var precomputedOccurrences = [5]int{665205, 1334028, 2667806, 4665104, 667857}

func TestRendezvousShardSelectorDistribution(t *testing.T) {
	// Distribution across multiple backends
	weights := []sharding.Shard{
		{Key: "a", Weight: 1},
		{Key: "b", Weight: 2},
		{Key: "c", Weight: 4},
		{Key: "d", Weight: 7},
		{Key: "e", Weight: 1},
	}
	s, err := sharding.NewRendezvousShardSelector(weights)
	require.NoError(t, err, "Selector construction should succeed")
	results := make([]int, len(precomputedResults))
	occurrences := make([]int, len(weights))
	

	// Request the shard for a very large amount of blobs
	for i := 0; i < COUNT; i++ {
		result := s.GetShard(uint64(i))
		if i < len(results) {
			results[i] = result
		}
		occurrences[result] += 1
	}

	t.Run("Distribution Error", func(t *testing.T) {
		// Requests should be fanned out with a small error margin.
		weightSum := uint32(0)
		for _, shard := range weights {
			weightSum += shard.Weight
		}
		for index, shard := range weights {
			require.InEpsilon(t, shard.Weight*COUNT/weightSum, occurrences[index], 1e-2)
		}
	})

	t.Run("Distribution Shape", func(t *testing.T) {
		shapeError := "The sharding algorithm has produced unexpected results, changing this distribution is a breaking change to buildbarn"
		require.Equal(t, precomputedResults[:], results, shapeError)
		require.Equal(t, precomputedOccurrences[:], occurrences, shapeError)
	})

	t.Run("Stability Test", func(t *testing.T) {
		// Removing a shard should only affect the shard that is removed
		results = make([]int, 10000)
		for i := 0; i < len(results); i++ {
			results[i] =  s.GetShard(uint64(i))
		}
		// drop the last shard in the slice
		weightsSubset := weights[:len(weights)-1]
		sharder, err := sharding.NewRendezvousShardSelector(weightsSubset)
		require.NoError(t, err, "Selector construction should succeed")
		for i := 0; i < len(results); i++ {
			result := sharder.GetShard(uint64(i))
			if results[i] == len(weights)-1 {
				continue
			}
			// result should be unchanged for all slices which did not resolve
			// to the dropped one
			require.Equal(t, results[i], result, "Dropping a shard should not effect other shards")
		}
	})
}
