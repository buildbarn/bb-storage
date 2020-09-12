package local

// BlockReference holds a stable reference to a block in a BlockList.
//
// BlockList uses simple integer indices to refer to blocks. This is
// useful, because these can be bounds checked and compared for
// (in)equality, which is a requirement for HashingKeyLocationMap and
// OldNewCurrentLocationBlobMap to function properly.
//
// What is problematic about these integer indices is that they become
// invalidated every time BlockList.PopFront() is called. This means
// that it's safe to use them as references to blocks within the context
// of a single operation while locks are held, but not to store them to
// refer to blocks later on (e.g., as done by LocationRecordArray).
//
// The goal of BlockReference is to act as a stable identifier for a
// block. Conversion functions are provided by BlockReferenceResolver to
// translate between BlockReferences and integer indices.
type BlockReference struct {
	// A version number of the layout of a BlockList. This version
	// number can be constructed by BlockList any way it sees fit.
	EpochID uint32

	// The number to subtract from the index of the last block
	// associated with this version of the layout of the BlockList.
	// More concretely, zero corresponds to the last block in that
	// epoch, one corresponds to the second-to-last block, etc..
	BlocksFromLast uint16
}

// BlockReferenceResolver is a helper type that can be used to convert
// between BlockReferences and integer indices of blocks managed by a
// BlockList.
//
// Both methods provided by this interface return a 64-bit hash seed.
// This value can be used by implementations of LocationRecordArray as a
// seed for checksum validation of individual entries. The idea behind
// using this value, as opposed to using some constant, is that it
// automatically causes entries for uncommitted changes to be discarded
// upon restart. Restarts after crashes may cause the same epoch ID to
// be reused, but the hash seed will differ.
type BlockReferenceResolver interface {
	// BlockReferenceToBlockIndex converts a BlockReference to an
	// integer index. The boolean value of this function indicates
	// whether the conversion is successful. Conversions may fail if
	// the epoch ID is too far in the past or in the future, or when
	// BlocksFromLast refers to a block that has already been
	// released.
	BlockReferenceToBlockIndex(blockReference BlockReference) (int, uint64, bool)

	// BlockIndexToBlockReference converts an integer index of a
	// block to a BlockReference. The BlockReference will use the
	// latest epoch ID.
	//
	// It is invalid to call this function with a block index that
	// is out of bounds.
	BlockIndexToBlockReference(blockIndex int) (BlockReference, uint64)
}
