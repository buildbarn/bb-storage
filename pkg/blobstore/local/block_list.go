package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// BlockListPutWriter is a callback that is returned by BlockList.Put().
// It can be used to store data corresponding to a blob in space that
// has been allocated. It is safe to call this function without holding
// any locks.
//
// This function blocks until all data contained in the Buffer has been
// processed or an error occurs. A BlockListPutFinalizer is returned
// that the caller must invoke while locked.
type BlockListPutWriter func(b buffer.Buffer) BlockListPutFinalizer

// BlockListPutFinalizer is returned by BlockListPutWriter after writing
// of data has finished. This function returns the offset at which the
// blob was stored. There is no guarantee that this data can be
// extracted again, as BlockList.PopFront() may have been called in the
// meantime.
type BlockListPutFinalizer func() (int64, error)

// BlockList keeps track of a list of blocks that are handed out by an
// underlying BlockAllocator. For every block, BlockList tracks how much
// space in the block is consumed.
//
// BlockList is only partially thread-safe. The BlockReferenceResolver
// methods and BlockList.Get() can be invoked in parallel (e.g., under a
// read lock), while BlockList.PopFront(), BlockList.PushBack(),
// BlockList.HasSpace(), BlockList.Put() and BlockListPutFinalizer must
// run exclusively (e.g., under a write lock). BlockListPutWriter is
// safe to call without holding any locks.
type BlockList interface {
	BlockReferenceResolver

	// PopFront removes the oldest block from the beginning of the
	// BlockList.
	PopFront()

	// PushBack adds a new block to the end of the BlockList.
	PushBack() error

	// Get a blob from a given block in the BlockList.
	Get(blockIndex int, digest digest.Digest, offsetBytes int64, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer

	// HasSpace returns whether a given block in the BlockList is
	// capable of storing an additional blob of a given size.
	HasSpace(blockIndex int, sizeBytes int64) bool

	// Put a new blob in a given block in the BlockList.
	Put(blockIndex int, sizeBytes int64) BlockListPutWriter
}
