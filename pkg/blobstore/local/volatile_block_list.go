package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/random"
)

type volatileBlockInfo struct {
	block         Block
	epochHashSeed uint64
}

type volatileBlockList struct {
	blockAllocator BlockAllocator

	blocks        []volatileBlockInfo
	oldestEpochID uint32
}

// NewVolatileBlockList creates a BlockList that is suitable for
// non-persistent data stores.
func NewVolatileBlockList(blockAllocator BlockAllocator) BlockList {
	return &volatileBlockList{
		blockAllocator: blockAllocator,

		oldestEpochID: 1,
	}
}

func (bl *volatileBlockList) BlockReferenceToBlockIndex(blockReference BlockReference) (int, uint64, bool) {
	// While PersistentBlockList allows one block to have zero or
	// more epochs, this implementation gets away with associating
	// every block with exactly one epoch.
	//
	// This allows us to obtain the block index and epoch hash seed
	// through simple subtractions, as opposed to requiring actual
	// lookups.
	epochIndex := blockReference.EpochID - bl.oldestEpochID
	if epochIndex >= uint32(len(bl.blocks)) {
		return 0, 0, false
	}
	blocksFromLast := uint32(blockReference.BlocksFromLast)
	if blocksFromLast > epochIndex {
		return 0, 0, false
	}
	return int(epochIndex - blocksFromLast), bl.blocks[epochIndex].epochHashSeed, true
}

func (bl *volatileBlockList) BlockIndexToBlockReference(blockIndex int) (BlockReference, uint64) {
	lastBlockIndex := len(bl.blocks) - 1
	return BlockReference{
		EpochID:        bl.oldestEpochID + uint32(lastBlockIndex),
		BlocksFromLast: uint16(lastBlockIndex - blockIndex),
	}, bl.blocks[lastBlockIndex].epochHashSeed
}

func (bl *volatileBlockList) PopFront() {
	bl.blocks[0].block.Release()
	bl.blocks[0].block = nil
	bl.blocks = bl.blocks[1:]
	bl.oldestEpochID++
}

func (bl *volatileBlockList) PushBack() error {
	block, _, err := bl.blockAllocator.NewBlock()
	if err != nil {
		return err
	}

	bl.blocks = append(bl.blocks, volatileBlockInfo{
		block:         block,
		epochHashSeed: random.CryptoThreadSafeGenerator.Uint64(),
	})
	return nil
}

func (bl *volatileBlockList) Get(index int, digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return bl.blocks[index].block.Get(digest, offsetBytes, sizeBytes, dataIntegrityCallback)
}

func (bl *volatileBlockList) HasSpace(index int, sizeBytes int64) bool {
	return bl.blocks[index].block.HasSpace(sizeBytes)
}

func (bl *volatileBlockList) Put(index int, sizeBytes int64) BlockListPutWriter {
	return bl.blocks[index].block.Put(sizeBytes)
}
