package sharding

// ShardSelector is an algorithm that for a hash resolves into an index which
// corresponds to the specific backend for that shard.
//
// The algorithm must be stable, the removal of an unavailable backend should
// not result in the reshuffling of any other blobs. It must also be
// numerically stable so that it produces the same result no matter the
// architecture.
type ShardSelector interface {
	GetShard(hash uint64) int
}

// Shard is a description of a shard. The shard selector will resolve to the
// same shard independent of the order of shards, but the returned index will
// correspond to the index sent to the ShardSelectors constructor.
type Shard struct {
	Key    string
	Weight uint32
}
