package legacy_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding/legacy"
	"github.com/stretchr/testify/require"
)

func TestWeightedShardPermuterDistribution(t *testing.T) {
	// Distribution across five backends with a total weight of 15.
	weights := []uint32{1, 4, 2, 5, 3}
	s := legacy.NewWeightedShardPermuter(weights)

	// Request a very long series of backends where a digest may be placed.
	occurrences := map[int]uint32{}
	round := 0
	s.GetShard(9127725482751685232, func(i int) bool {
		require.True(t, i < len(weights))
		occurrences[i]++
		round++
		return round < 1000000
	})

	// Requests should be fanned out with a small error margin.
	for shard, weight := range weights {
		require.InEpsilon(t, weight*1000000/15, occurrences[shard], 0.01)
	}
}
