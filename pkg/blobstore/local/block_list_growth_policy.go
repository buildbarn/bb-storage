package local

// BlockListGrowthPolicy is used by OldCurrentNewLocationBlobMap to
// determine whether the number of Blocks in the underlying BlockList is
// allowed to grow.
type BlockListGrowthPolicy interface {
	ShouldGrowNewBlocks(currentBlocks, newBlocks int) bool
	ShouldGrowCurrentBlocks(currentBlocks int) bool
}

type immutableBlockListGrowthPolicy struct {
	desiredCurrentAndNewBlocks int
}

// NewImmutableBlockListGrowthPolicy creates a BlockListGrowthPolicy
// that is suitable for data stores that hold objects that are
// immutable, such as the Content Addressable Storage (CAS).
//
// This policy permits new objects to be written to multiple Blocks,
// which is good for ensuring that data is spread out evenly. This
// amortizes the cost of refreshing these objects in the future.
//
// It also allows the number of "new" blocks to exceed the configured
// maximum in case the number of "current" blocks is low, increasing the
// spread of data even further.
func NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks int) BlockListGrowthPolicy {
	return immutableBlockListGrowthPolicy{
		desiredCurrentAndNewBlocks: currentBlocks + newBlocks,
	}
}

func (gp immutableBlockListGrowthPolicy) ShouldGrowNewBlocks(currentBlocks, newBlocks int) bool {
	return currentBlocks+newBlocks < gp.desiredCurrentAndNewBlocks
}

func (gp immutableBlockListGrowthPolicy) ShouldGrowCurrentBlocks(currentBlocks int) bool {
	return false
}

type mutableBlockListGrowthPolicy struct {
	desiredCurrentBlocks int
}

// NewMutableBlockListGrowthPolicy creates a BlockListGrowthPolicy
// that is suitable for data stores that hold objects that are
// mutable, such as the Action Cache (AC). Calls such as
// UpdateActionResult() are expected to replace existing entries.
//
// This policy only permits new objects to be written to the latest
// Block. This ensures that updating the corresponding entry in the
// KeyLocationMap is guaranteed to succeed.
func NewMutableBlockListGrowthPolicy(currentBlocks int) BlockListGrowthPolicy {
	return mutableBlockListGrowthPolicy{
		desiredCurrentBlocks: currentBlocks,
	}
}

func (gp mutableBlockListGrowthPolicy) ShouldGrowNewBlocks(currentBlocks, newBlocks int) bool {
	return newBlocks < 1
}

func (gp mutableBlockListGrowthPolicy) ShouldGrowCurrentBlocks(currentBlocks int) bool {
	return currentBlocks < gp.desiredCurrentBlocks
}
