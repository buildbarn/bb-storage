package sharding_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/stretchr/testify/require"
)

func TestRendezvousShardSelectorDistribution(t *testing.T) {
	// Distribution across five backends with a total weight of 15.
	weights := map[string]uint32{"a": 1, "b": 4, "c:": 2, "d": 5, "e": 3}
	s := sharding.NewRendezvousShardSelector(weights)

	// Request the shard for a very large amount of blobs
	occurrences := map[string]uint32{}
	for i := 0; i < 1000000; i++ {
		hash := uint64(i)
		occurrences[s.GetShard(hash)] += 1
	}

	// Requests should be fanned out with a small error margin.
	for shard, weight := range weights {
		require.InEpsilon(t, weight*1000000/15, occurrences[shard], 0.01)
	}
}
