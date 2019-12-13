package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
)

// Block of storage that contains a sequence of blobs.
type Block interface {
	Get(offset int64, sizeBytes int64) buffer.Buffer
	Put(offset int64, b buffer.Buffer) error
}

// BlockAllocator is used by LocalBlobAccess to allocate large blocks of
// storage (in-memory or on-disk) at a time. These blocks are then
// filled with blobs that are stored without any padding in between.
type BlockAllocator interface {
	NewBlock() Block
}
