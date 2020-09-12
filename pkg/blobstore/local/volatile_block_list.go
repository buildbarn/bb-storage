package local

import (
	"math/rand"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// sharedBlock is a reference counted Block. Whereas Block can only be
// released once, sharedBlock has a pair of acquire() and release()
// functions. This type is used for being able to call Put() on a Block
// in such a way that the underlying Block does not disappear.
//
// The reference count stored by sharedBlock is not updated atomically.
// It relies on locking to be performed at a higher level.
type sharedBlock struct {
	block    Block
	refcount uint64
}

func newSharedBlock(block Block) *sharedBlock {
	return &sharedBlock{
		block:    block,
		refcount: 1,
	}
}

func (sb *sharedBlock) acquire() {
	if sb.refcount == 0 {
		panic("Invalid reference count")
	}
	sb.refcount++
}

func (sb *sharedBlock) release() {
	if sb.refcount == 0 {
		panic("Invalid reference count")
	}
	sb.refcount--
	if sb.refcount == 0 {
		sb.block.Release()
	}
}

type volatileBlockInfo struct {
	block                   *sharedBlock
	allocationOffsetSectors int64
	epochHashSeed           uint64
}

type volatileBlockList struct {
	blockAllocator   BlockAllocator
	sectorSizeBytes  int
	blockSectorCount int64

	blocks        []volatileBlockInfo
	oldestEpochID uint32
}

// NewVolatileBlockList creates a BlockList that is suitable for
// non-persistent data stores.
func NewVolatileBlockList(blockAllocator BlockAllocator, sectorSizeBytes int, blockSectorCount int64) BlockList {
	return &volatileBlockList{
		blockAllocator:   blockAllocator,
		sectorSizeBytes:  sectorSizeBytes,
		blockSectorCount: blockSectorCount,

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
	bl.blocks[0].block.release()
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
		block:         newSharedBlock(block),
		epochHashSeed: rand.Uint64(),
	})
	return nil
}

func (bl *volatileBlockList) Get(index int, digest digest.Digest, offsetBytes int64, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return bl.blocks[index].block.block.Get(digest, offsetBytes, sizeBytes, dataIntegrityCallback)
}

func (bl *volatileBlockList) toSectors(sizeBytes int64) int64 {
	// Determine the number of sectors needed to store the object.
	//
	// TODO: This can be wasteful for storing small objects with
	// large sector sizes. Should we add logic for packing small
	// objects together into a single sector?
	return (sizeBytes + int64(bl.sectorSizeBytes) - 1) / int64(bl.sectorSizeBytes)
}

func (bl *volatileBlockList) HasSpace(index int, sizeBytes int64) bool {
	blockInfo := &bl.blocks[index]
	return bl.blockSectorCount-blockInfo.allocationOffsetSectors >= bl.toSectors(sizeBytes)
}

func (bl *volatileBlockList) Put(index int, sizeBytes int64) BlockListPutWriter {
	blockInfo := &bl.blocks[index]
	offsetBytes := blockInfo.allocationOffsetSectors * int64(bl.sectorSizeBytes)
	blockInfo.allocationOffsetSectors += bl.toSectors(sizeBytes)

	block := blockInfo.block
	block.acquire()
	return func(b buffer.Buffer) BlockListPutFinalizer {
		err := block.block.Put(offsetBytes, b)
		return func() (int64, error) {
			block.release()
			return offsetBytes, err
		}
	}
}
