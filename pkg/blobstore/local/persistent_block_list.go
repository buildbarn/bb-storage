package local

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/random"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errClosedForWriting is an error code to indicate that the BlockList does not
// accept any more write operations and ongoing write operations will fail.
var errClosedForWriting = status.Error(codes.Unavailable, "Cannot write object to storage, as storage is shutting down")

// notificationChannel is a helper type to manage the channels returned
// by GetBlockReleaseWakeup and GetBlockPutWakeup. For each of the
// channels that these functions hand out, we must make sure that we
// call close() exactly once.
//
// Forgetting to call close() may cause PeriodicSyncer's goroutines to
// get stuck indefinitely. Calling close() more than once causes us to
// crash.
type notificationChannel struct {
	channel    chan struct{}
	isBlocking bool
}

func newNotificationChannel() notificationChannel {
	return notificationChannel{
		channel:    make(chan struct{}, 1),
		isBlocking: true,
	}
}

func (nc *notificationChannel) block() {
	if !nc.isBlocking {
		*nc = newNotificationChannel()
	}
}

func (nc *notificationChannel) unblock() {
	if nc.isBlocking {
		close(nc.channel)
		nc.isBlocking = false
	}
}

type persistentBlockInfo struct {
	block         Block
	blockLocation *pb.BlockLocation

	// The offsets at which new data needs to be stored, the highest
	// offset at which writes actually completed and how much of it
	// has actually been persisted.
	//
	// At any given point in time, the following inequality holds:
	//
	//        synchronizedOffsetBytes
	//     <= synchronizingOffsetBytes
	//     <= writtenOffsetBytes
	writtenOffsetBytes       int64
	synchronizingOffsetBytes int64
	synchronizedOffsetBytes  int64

	// The number of epochs that were created while this was the
	// last block. This is used to efficiently remove old epochs in
	// PopFront().
	epochCount int
}

// PersistentBlockList is an implementation of BlockList whose internal
// state can be extracted and persisted. This allows data contained in
// its blocks to be accessed after restarts.
type PersistentBlockList struct {
	blockAllocator BlockAllocator
	// When closedForWriting is set, BlockList will return errClosedForWriting
	// for all future calls to PushBack and Put. This also applies to any
	// ongoing calls. closedForWriting can only transition from false to true.
	closedForWriting bool

	// Blocks that are currently available for reading and writing.
	blocks []persistentBlockInfo

	// Epochs for which data can still be read back. For each epoch,
	// we track the hash seed, which is needed by
	// BlockDeviceBackedLocationRecordArray to perform checksum
	// validation on a record. We also track a last block index.
	// This refers to the last block in the blocks list above for
	// which data can be read for that epoch.
	//
	// These two lists always have the same length. Ideally, they
	// would be declared as a single list. Unfortunately, this
	// wouldn't allow GetPersistentState() to get contiguous lists
	// of hash seeds.
	epochHashSeeds              []uint64
	epochLastAbsoluteBlockIndex []int
	totalBlocksReleased         int

	// Epoch IDs of the one in which new writes take place, the one
	// for which data is currently being synchronized to storage,
	// and the last that was synchronized entirely.
	//
	// At any given point in time, the following inequality holds:
	//
	//        synchronizedEpochs
	//     <= synchronizingEpochs
	//     <= len(bl.epoch{HashSeeds,LastAbsoluteBlockIndex})
	oldestEpochID       uint32
	synchronizingEpochs int
	synchronizedEpochs  int
	blockPutWakeup      notificationChannel

	// Information pertaining to which blocks need to be released,
	// but whose information has not been removed from persistent
	// state yet. Releasing the blocks needs to be delayed, because
	// we don't want to write new data to blocks that are still
	// referenced from the old persistent state. That would cause
	// data integrity errors after restarts.
	//
	// Whenever we need to release a block, we request that
	// persistent state is updated immediately. This is done to
	// ensure block allocation doesn't start failing.
	blocksToRelease    []Block
	blocksReleasing    int
	blockReleaseWakeup notificationChannel
}

// NewPersistentBlockList provides an implementation of BlockList whose
// state can be persisted. This makes it possible to preserve the
// contents of FlatBlobAccess and HierarchicalCASBlobAccess across
// restarts.
func NewPersistentBlockList(blockAllocator BlockAllocator, initialOldestEpochID uint32, initialBlocks []*pb.BlockState) (*PersistentBlockList, int) {
	bl := &PersistentBlockList{
		blockAllocator: blockAllocator,

		blockPutWakeup:     newNotificationChannel(),
		blockReleaseWakeup: newNotificationChannel(),
	}

	// Attempt to restore blocks from a previous run.
	for _, blockState := range initialBlocks {
		block, found := blockAllocator.NewBlockAtLocation(blockState.BlockLocation, blockState.WriteOffsetBytes)
		if !found {
			// Persistence state references an unknown
			// block. Skip the remaining blocks.
			break
		}

		// Restore all epochs for which this block is last.
		bl.epochHashSeeds = append(bl.epochHashSeeds, blockState.EpochHashSeeds...)
		for range blockState.EpochHashSeeds {
			bl.epochLastAbsoluteBlockIndex = append(bl.epochLastAbsoluteBlockIndex, len(bl.blocks))
		}

		bl.blocks = append(bl.blocks, persistentBlockInfo{
			block:                    block,
			blockLocation:            blockState.BlockLocation,
			writtenOffsetBytes:       blockState.WriteOffsetBytes,
			synchronizingOffsetBytes: blockState.WriteOffsetBytes,
			synchronizedOffsetBytes:  blockState.WriteOffsetBytes,
			epochCount:               len(blockState.EpochHashSeeds),
		})
	}

	// Continue at the epoch where the previous run left off.
	bl.oldestEpochID = initialOldestEpochID
	bl.synchronizingEpochs = len(bl.epochHashSeeds)
	bl.synchronizedEpochs = len(bl.epochHashSeeds)
	return bl, len(bl.blocks)
}

var (
	_ BlockList             = (*PersistentBlockList)(nil)
	_ PersistentStateSource = (*PersistentBlockList)(nil)
)

// BlockReferenceToBlockIndex converts a BlockReference to the index of
// the block in the BlockList. This conversion may fail if the block has
// already been released using PopFront().
func (bl *PersistentBlockList) BlockReferenceToBlockIndex(blockReference BlockReference) (int, uint64, bool) {
	// Look up information for the provided epoch.
	epochIndex := blockReference.EpochID - bl.oldestEpochID
	if epochIndex >= uint32(len(bl.epochHashSeeds)) {
		return 0, 0, false
	}

	// Look up the block index based on the last block index of the
	// requested epoch.
	blocksFromLast := int(blockReference.BlocksFromLast)
	lastBlockIndex := bl.epochLastAbsoluteBlockIndex[epochIndex] - bl.totalBlocksReleased
	if blocksFromLast > lastBlockIndex {
		return 0, 0, false
	}
	return lastBlockIndex - blocksFromLast, bl.epochHashSeeds[epochIndex], true
}

// BlockIndexToBlockReference converts the index of a block to a
// BlockReference that uses the latest epoch ID.
func (bl *PersistentBlockList) BlockIndexToBlockReference(blockIndex int) (BlockReference, uint64) {
	lastEpochIndex := len(bl.epochHashSeeds) - 1
	return BlockReference{
		EpochID:        bl.oldestEpochID + uint32(lastEpochIndex),
		BlocksFromLast: uint16(bl.epochLastAbsoluteBlockIndex[lastEpochIndex] - bl.totalBlocksReleased - blockIndex),
	}, bl.epochHashSeeds[lastEpochIndex]
}

// PopFront removes the oldest block from the BlockList, having index
// zero.
func (bl *PersistentBlockList) PopFront() {
	// Remove the first block, but don't release the block
	// immediately. It needs to be removed from the persistent state
	// first, as the BlockAllocator would otherwise start handing it
	// out again. This needs to be done as soon as possible, as
	// PushBack() may start to fail otherwise.
	firstBlock := &bl.blocks[0]
	bl.blocks = bl.blocks[1:]
	bl.blocksToRelease = append(bl.blocksToRelease, firstBlock.block)
	firstBlock.block = nil
	bl.blockReleaseWakeup.unblock()

	// Remove all epochs that no longer refer to any blocks in the
	// current block list. Those can no longer be used to look up
	// any data.
	bl.oldestEpochID += uint32(firstBlock.epochCount)
	bl.epochHashSeeds = bl.epochHashSeeds[firstBlock.epochCount:]
	bl.epochLastAbsoluteBlockIndex = bl.epochLastAbsoluteBlockIndex[firstBlock.epochCount:]
	bl.totalBlocksReleased++

	// Adjust counts of how many epochs have been synchronized.
	if firstBlock.epochCount >= bl.synchronizingEpochs {
		bl.synchronizingEpochs = 0
	} else {
		bl.synchronizingEpochs -= firstBlock.epochCount
	}
	if firstBlock.epochCount >= bl.synchronizedEpochs {
		bl.synchronizedEpochs = 0
	} else {
		bl.synchronizedEpochs -= firstBlock.epochCount
	}
	if bl.synchronizedEpochs == len(bl.epochHashSeeds) {
		bl.blockPutWakeup.block()
	}
}

// PushBack appends a new block to the BlockList. The block is obtained
// by calling into the underlying BlockAllocator.
func (bl *PersistentBlockList) PushBack() error {
	if bl.closedForWriting {
		return errClosedForWriting
	}

	block, location, err := bl.blockAllocator.NewBlock()
	if err != nil {
		return err
	}

	bl.blocks = append(bl.blocks, persistentBlockInfo{
		block:         block,
		blockLocation: location,
	})
	return nil
}

// Get data from one of the blocks managed by this BlockList.
func (bl *PersistentBlockList) Get(index int, digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	return bl.blocks[index].block.Get(digest, offsetBytes, sizeBytes, dataIntegrityCallback)
}

// HasSpace returns whether a block with a given index has sufficient
// space to store a blob of a given size.
func (bl *PersistentBlockList) HasSpace(index int, sizeBytes int64) bool {
	return bl.blocks[index].block.HasSpace(sizeBytes)
}

// Put data into a block managed by the BlockList.
func (bl *PersistentBlockList) Put(index int, sizeBytes int64) BlockListPutWriter {
	if bl.closedForWriting {
		return func(b buffer.Buffer) BlockListPutFinalizer {
			b.Discard()
			return func() (int64, error) {
				return 0, errClosedForWriting
			}
		}
	}

	// Allocate space from the requested block.
	putWriter := bl.blocks[index].block.Put(sizeBytes)
	absoluteBlockIndex := bl.totalBlocksReleased + index
	return func(b buffer.Buffer) BlockListPutFinalizer {
		// Copy data into the block without holding any locks.
		putFinalizer := putWriter(b)

		return func() (int64, error) {
			offsetBytes, err := putFinalizer()
			if err != nil {
				return 0, err
			}
			if bl.closedForWriting {
				return 0, errClosedForWriting
			}

			// Adjust information on how much data has
			// actually been written into this block. This
			// information needs to be persisted to ensure
			// we start allocating at the right offset after
			// restarts.
			if absoluteBlockIndex < bl.totalBlocksReleased {
				return 0, status.Error(codes.Internal, "The block to which this blob was written, has already been released")
			}
			blockInfo := &bl.blocks[absoluteBlockIndex-bl.totalBlocksReleased]
			if writtenOffsetBytes := offsetBytes + sizeBytes; blockInfo.writtenOffsetBytes < writtenOffsetBytes {
				blockInfo.writtenOffsetBytes = writtenOffsetBytes
			}

			// At some point in the nearby future, we will
			// see a call to BlockIndexToBlockReference()
			// for this blob. We will need to make sure that
			// the resulting BlockReference uses the right
			// epoch ID. There are two cases in which we
			// need to bump the epoch ID:
			//
			// 1. All of the data corresponding to the
			//    current epoch is already being
			//    synchronized. No more blobs may be added
			//    to this epoch.
			// 2. The current epoch was started before the
			//    block in which data was stored was
			//    created. This means the current epoch
			//    cannot reference the block.
			if len(bl.epochLastAbsoluteBlockIndex) == bl.synchronizingEpochs || bl.epochLastAbsoluteBlockIndex[len(bl.epochLastAbsoluteBlockIndex)-1] < absoluteBlockIndex {
				bl.epochHashSeeds = append(bl.epochHashSeeds, random.CryptoThreadSafeGenerator.Uint64())
				bl.epochLastAbsoluteBlockIndex = append(bl.epochLastAbsoluteBlockIndex, bl.totalBlocksReleased+len(bl.blocks)-1)
				bl.blocks[len(bl.blocks)-1].epochCount++

				// We now have new data that can be
				// synchronized to storage.
				bl.blockPutWakeup.unblock()
			}
			return offsetBytes, err
		}
	}
}

// GetBlockReleaseWakeup returns a channel that triggers when there are
// one or more blocks that have been released since the last persistent
// state was written to disk.
func (bl *PersistentBlockList) GetBlockReleaseWakeup() <-chan struct{} {
	return bl.blockReleaseWakeup.channel
}

// GetBlockPutWakeup returns a channel that triggers when there was data
// stored in one of the blocks since the last persistent state was
// written to disk.
func (bl *PersistentBlockList) GetBlockPutWakeup() <-chan struct{} {
	return bl.blockPutWakeup.channel
}

// NotifySyncStarting needs to be called right before the data on the
// storage medium underneath the BlockAllocator is synchronized. This
// causes the epoch ID to be increased when the next blob is stored.
func (bl *PersistentBlockList) NotifySyncStarting(isFinalSync bool) {
	if isFinalSync {
		bl.closedForWriting = true
	}
	// Preserve the current epoch ID and the amount of data written
	// into every block.
	bl.synchronizingEpochs = len(bl.epochHashSeeds)
	for i := range bl.blocks {
		bl.blocks[i].synchronizingOffsetBytes = bl.blocks[i].writtenOffsetBytes
	}
}

// NotifySyncCompleted needs to be called right after the data on the
// storage medium underneath the BlockAllocator is synchronized. This
// causes the next call to GetPersistentState() to return information on
// the newly synchronized data.
func (bl *PersistentBlockList) NotifySyncCompleted() {
	// Expose the preserved epoch ID and the amount of data written
	// into every block as part of GetPersistentState().
	bl.synchronizedEpochs = bl.synchronizingEpochs
	if bl.synchronizedEpochs == len(bl.epochHashSeeds) {
		bl.blockPutWakeup.block()
	}
	for i := range bl.blocks {
		bl.blocks[i].synchronizedOffsetBytes = bl.blocks[i].synchronizingOffsetBytes
	}
}

// GetPersistentState returns information that needs to be persisted to
// disk to be able to restore the layout of the BlockList after a
// restart.
func (bl *PersistentBlockList) GetPersistentState() (uint32, []*pb.BlockState) {
	// Create a list of all of the epochs that we've synchronized
	// properly. Partition the epochs by the block that was the last
	// block at the time the epoch was created. This gives a compact
	// representation, where each block and epoch only needs to be
	// described once.
	//
	// There is no need to emit trailing blocks that contain no
	// epochs, as those don't contain any data that has been
	// persisted properly.
	blocks := make([]*pb.BlockState, 0, len(bl.blocks))
	for blockIndex, lastEpochIndex := 0, 0; lastEpochIndex < bl.synchronizedEpochs; blockIndex++ {
		// Determine which epoch hash seeds were created as part
		// of this block.
		firstEpochIndex := lastEpochIndex
		lastEpochIndex += bl.blocks[blockIndex].epochCount
		if lastEpochIndex > bl.synchronizedEpochs {
			lastEpochIndex = bl.synchronizedEpochs
		}

		blocks = append(blocks, &pb.BlockState{
			BlockLocation:    bl.blocks[blockIndex].blockLocation,
			WriteOffsetBytes: bl.blocks[blockIndex].synchronizedOffsetBytes,
			EpochHashSeeds:   bl.epochHashSeeds[firstEpochIndex:lastEpochIndex],
		})
	}

	// Store which blocks we're removing from the persistent state.
	// This allows NotifyPersistentStateWritten() to remove them
	// from our bookkeeping, thereby allowing PushBack() to start
	// using those blocks again.
	bl.blocksReleasing = len(bl.blocksToRelease)
	return bl.oldestEpochID, blocks
}

// NotifyPersistentStateWritten needs to be called after the data
// returned by GetPersistentState() is written to disk. This allows
// PersistentBlockList to recycle blocks that were used previously.
func (bl *PersistentBlockList) NotifyPersistentStateWritten() {
	// Definitively release blocks that were still referenced by the
	// previous version of the persistent state. It is now safe to
	// let PushBack() reuse these blocks.
	for i := 0; i < bl.blocksReleasing; i++ {
		bl.blocksToRelease[i].Release()
		bl.blocksToRelease[i] = nil
	}
	bl.blocksToRelease = bl.blocksToRelease[bl.blocksReleasing:]
	bl.blocksReleasing = 0
	if len(bl.blocksToRelease) == 0 {
		bl.blockReleaseWakeup.block()
	}
}
