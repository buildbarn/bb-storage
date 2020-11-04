package local

import (
	"bytes"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type inMemoryBlockAllocator struct {
	blockSize  int
	nextOffset int64
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

func (ia *inMemoryBlockAllocator) NewBlock() (Block, int64, error) {
	// Consumers of BlockAllocator require that every block has a
	// unique offset. Satisfy this contract by handing out made-up
	// offsets.
	offset := ia.nextOffset
	ia.nextOffset += int64(ia.blockSize)

	return inMemoryBlock{
		data: make([]byte, ia.blockSize),
	}, offset, nil
}

func (ia *inMemoryBlockAllocator) NewBlockAtOffset(offset int64) (Block, bool) {
	// There is no way to access old blocks again.
	return nil, false
}

type inMemoryBlock struct {
	data []byte
}

func (ib inMemoryBlock) Get(digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return buffer.NewValidatedBufferFromByteSlice(ib.data[offsetBytes : offsetBytes+sizeBytes])
}

func (ib inMemoryBlock) Put(offsetBytes int64, b buffer.Buffer) error {
	return b.IntoWriter(bytes.NewBuffer(ib.data[offsetBytes:offsetBytes]))
}

func (ib inMemoryBlock) Release() {}
