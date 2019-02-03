package sharding

import (
	"sort"

	"github.com/lazybeaver/xorshift"
)

type weightedShardPermuter struct {
	cumulativeWeights []uint64
}

// NewWeightedShardPermuter is a shard selection algorithm that
// generates a permutation of [0, len(weights)) for every hash, where
// every index i is returned weights[i] times. This makes it possible to
// have storage backends with different specifications in terms of
// capacity and throughput, giving them a proportional amount of
// traffic.
func NewWeightedShardPermuter(weights []uint32) ShardPermuter {
	// Compute cumulative weights for binary searching.
	var cumulativeWeights []uint64
	totalWeight := uint64(0)
	for _, weight := range weights {
		totalWeight += uint64(weight)
		cumulativeWeights = append(cumulativeWeights, totalWeight)
	}
	s := &weightedShardPermuter{
		cumulativeWeights: cumulativeWeights,
	}
	return s
}

func (s *weightedShardPermuter) GetShard(hash uint64, selector ShardSelector) {
	if hash == 0 {
		hash = 1
	}
	sequence := xorshift.NewXorShift64Star(hash)
	for {
		// Perform binary search to find corresponding backend.
		slot := sequence.Next() % s.cumulativeWeights[len(s.cumulativeWeights)-1]
		idx := sort.Search(len(s.cumulativeWeights), func(i int) bool {
			return slot < s.cumulativeWeights[i]
		})
		if !selector(idx) {
			return
		}
	}
}
