package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
)

// BlockPutWriter is a callback that is returned by Block.Put(). It can
// be used to store data corresponding to a blob in space that has been
// allocated. It is safe to call this function without holding any
// locks.
//
// This function blocks until all data contained in the Buffer has been
// processed or an error occurs. A BlockPutFinalizer is returned that
// the caller must invoke while locked.
type BlockPutWriter func(b buffer.Buffer) BlockPutFinalizer

// BlockPutFinalizer is returned by BlockPutWriter after writing of data
// has finished. This function returns the offset at which the blob was
// stored.
type BlockPutFinalizer func() (int64, error)

// Block of storage that contains a sequence of blobs. Buffers returned
// by Get() must remain valid, even if Release() is called.
type Block interface {
	Get(digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
	HasSpace(sizeBytes int64) bool
	Put(sizeBytes int64) BlockPutWriter
	Release()
}

// BlockAllocator is used by BlockList to allocate large blocks of
// storage (in-memory or on-disk) at a time. These blocks are then
// filled with blobs that are stored without any padding in between.
//
// The methods provided this interface are not thread-safe. Exclusive
// locking must be used when allocating blocks.
type BlockAllocator interface {
	// Used to allocate a fresh block of data. The location at which
	// this block is stored is returned, both to allow the caller to
	// store this information as part of persistent state and to
	// detect recycling of blocks that were used previously.
	//
	// If persistent storage is not supported, nil may be returned.
	NewBlock() (Block, *pb.BlockLocation, error)

	// Used to obtain a block of data at an explicit location. This is
	// called when attempting to reuse previous persistent state.
	//
	// This function may fail if no free block at this location
	// exists, or if persistent storage is not provided.
	NewBlockAtLocation(location *pb.BlockLocation, writeOffsetBytes int64) (Block, bool)
}
