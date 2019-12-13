package local

import (
	"bytes"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
)

type inMemoryBlockAllocator struct {
	blockSize int
}

// NewInMemoryBlockAllocator creates a block allocator that stores its
// blocks directly in memory, being backed by a simple byte slice. The
// byte slice is already fully allocated. It does not grow to the
// desired size lazily.
func NewInMemoryBlockAllocator(blockSize int) BlockAllocator {
	return &inMemoryBlockAllocator{
		blockSize: blockSize,
	}
}

func (ia *inMemoryBlockAllocator) NewBlock() Block {
	return inMemoryBlock{
		data: make([]byte, ia.blockSize),
	}
}

type inMemoryBlock struct {
	data []byte
}

func (ib inMemoryBlock) Get(offset int64, sizeBytes int64) buffer.Buffer {
	return buffer.NewValidatedBufferFromByteSlice(ib.data[offset : offset+sizeBytes])
}

func (ib inMemoryBlock) Put(offset int64, b buffer.Buffer) error {
	return b.IntoWriter(bytes.NewBuffer(ib.data[offset:offset]))
}
