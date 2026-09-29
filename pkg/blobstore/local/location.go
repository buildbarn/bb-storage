package local

// Location at which a blob is stored within blocks managed by
// implementations of BlockList. A location consists of a number that
// identifies a block in a BlockList and the region within the block.
type Location struct {
	BlockIndex  int
	OffsetBytes int64
	SizeBytes   int64
}
