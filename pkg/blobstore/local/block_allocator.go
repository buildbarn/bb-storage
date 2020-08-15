package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// Block of storage that contains a sequence of blobs. Buffers returned
// by Get() must remain valid, even if Release() is called.
type Block interface {
	Get(digest digest.Digest, offsetBytes int64, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer
	Put(offsetBytes int64, b buffer.Buffer) error
	Release()
}

// BlockAllocator is used by LocalBlobAccess to allocate large blocks of
// storage (in-memory or on-disk) at a time. These blocks are then
// filled with blobs that are stored without any padding in between.
type BlockAllocator interface {
	NewBlock() (Block, error)
}
