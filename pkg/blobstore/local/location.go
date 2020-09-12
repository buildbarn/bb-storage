package local

// Location at which a blob is stored within blocks managed by
// implementations of BlockList. A location consists of a number that
// identifies a block in a BlockList and the region within the block.
type Location struct {
	BlockIndex  int
	OffsetBytes int64
	SizeBytes   int64
}

// IsOlder returns true if the receiving Location is stored in Block
// that is older than the Location argument, or if it is stored prior to
// the Location argument within the same Block.
func (a Location) IsOlder(b Location) bool {
	return a.BlockIndex < b.BlockIndex || (a.BlockIndex == b.BlockIndex && a.OffsetBytes < b.OffsetBytes)
}
