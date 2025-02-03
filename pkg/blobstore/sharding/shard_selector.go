package sharding

// ShardSelector is an algorithm that for a hash resolves into a key which
// corresponds to the specific backend for that shard.
//
// The algorithm must be stable, the removal of an unavailable backend should
// not result in the reshuffling of any other blobs.
type ShardSelector interface {
	GetShard(hash uint64) string
}
