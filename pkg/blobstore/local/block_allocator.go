package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Block of storage that contains a sequence of blobs. Buffers returned
// by Get() must remain valid, even if Release() is called.
type Block interface {
	Get(digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
	Put(offsetBytes int64, b buffer.Buffer) error
	Release()
}

// BlockAllocator is used by BlockList to allocate large blocks of
// storage (in-memory or on-disk) at a time. These blocks are then
// filled with blobs that are stored without any padding in between.
//
// The methods provided this interface are not thread-safe. Exclusive
// locking must be used when allocating blocks.
type BlockAllocator interface {
	// Used to allocate a fresh block of data. The offset at which
	// this block is stored is returned, both to allow the caller to
	// store this information as part of persistent state and to
	// detect recycling of blocks that were used previously.
	//
	// If persistent storage is not supported, a made up offset
	// (e.g., an incrementing counter value) needs to be returned.
	NewBlock() (Block, int64, error)

	// Used to obtain a block of data at an explicit offset. This is
	// called when attempting to reuse previous persistent state.
	//
	// This function may fail if no free block at this offset
	// exists, or if persistent storage is not provided.
	NewBlockAtOffset(offset int64) (Block, bool)
}
