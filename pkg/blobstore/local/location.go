package local

// Location at which a blob is stored within blocks managed by a
// LocalBlobAccess. A location consists of a number that identifies a
// block and the region within the block.
type Location struct {
	BlockID     int
	OffsetBytes int64
	SizeBytes   int64
}

// IsOlder returns true if the receiving Location is stored in Block
// that is older than the Location argument, or if it is stored prior to
// the Location argument within the same Block.
func (a Location) IsOlder(b Location) bool {
	return a.BlockID < b.BlockID || (a.BlockID == b.BlockID && a.OffsetBytes < b.OffsetBytes)
}

// LocationValidator assesses whether a Location of where a blob is
// stored is still valid. It does this by bounds checking the ID number
// of the block.
type LocationValidator struct {
	OldestValidBlockID int
	NewestValidBlockID int
}

// IsValid returns whether the provided Location still refers to a place
// where the data corresponding to this blob may be retrieved.
func (v *LocationValidator) IsValid(l Location) bool {
	return l.BlockID >= v.OldestValidBlockID && l.BlockID <= v.NewestValidBlockID
}
