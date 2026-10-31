package local

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
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

func (ia *inMemoryBlockAllocator) NewBlock() (Block, *pb.BlockLocation, error) {
	return &inMemoryBlock{
		data: make([]byte, ia.blockSize),
	}, nil, nil
}

func (inMemoryBlockAllocator) NewBlockAtLocation(location *pb.BlockLocation, writeOffsetBytes int64) (Block, bool) {
	// There is no way to access old blocks again.
	return nil, false
}

type inMemoryBlock struct {
	data             []byte
	writeOffsetBytes int
}

func (ib *inMemoryBlock) Get(digest digest.Digest, offsetBytes, sizeBytes int64) ([]byte, error) {
	return ib.data[offsetBytes : offsetBytes+sizeBytes], nil
}

func (ib *inMemoryBlock) HasSpace(sizeBytes int64) bool {
	return int64(len(ib.data)-ib.writeOffsetBytes) >= sizeBytes
}

func (ib *inMemoryBlock) Put(sizeBytes int64) BlockPutWriter {
	// Allocate space.
	offsetBytes := ib.writeOffsetBytes
	ib.writeOffsetBytes += int(sizeBytes)
	return func(data []byte) BlockPutFinalizer {
		copy(ib.data[offsetBytes:offsetBytes+int(sizeBytes)], data)
		return func() (int64, error) {
			return int64(offsetBytes), nil
		}
	}
}

func (inMemoryBlock) Release() {}
