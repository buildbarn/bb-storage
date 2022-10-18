package local

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	oldCurrentNewLocationBlobMapPrometheusMetrics sync.Once

	oldCurrentNewLocationBlobMapLastRemovedOldBlockInsertionTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "old_new_current_location_blob_map_last_removed_old_block_insertion_time_seconds",
			Help:      "Time at which the last removed block was inserted into the \"old\" queue, which is an indicator for the worst-case blob retention time",
		},
		[]string{"storage_type"})
)

type oldBlockState struct {
	insertionTime float64
}

// OldCurrentNewLocationBlobMap is a LocationBlobMap that stores data in
// blocks. Blocks are managed using a BlockList. Blobs cannot span
// multiple blocks, meaning that blocks generally need to be large in
// size (gigabytes). The number of blocks may be relatively low. For
// example, for a 512 GiB cache, it is acceptable to create 32 blocks of
// 16 GiB in size.
//
// Blocks are partitioned into three groups based on their creation
// time, named "old", "current" and "new". Blobs provided to Put() will
// always be stored in a block in the "new" group. When the oldest block
// in the "new" group becomes full, it is moved to the "current" group.
// This causes the oldest block in the "current" group to be displaced
// to the "old" group. The oldest block in the "old" group is discarded.
//
// The difference between the "current" group and the "old" group is
// that the needRefresh value returned by Get() differs.
// Data in the "old" group is at risk of being removed in the nearby
// future, which is why it needs to be copied into the "new" group when
// requested to be retained. Data in the "current" group is assumed to
// remain present for the time being, which is why it is left in place.
// This copying is performed by FlatBlobAccess.
//
// Below is an illustration of how the blocks of data may be laid out at
// a given point in time. Every column of █ characters corresponds to a
// single block. The number of characters indicates the amount of data
// stored within.
//
//	← Over time, blocks move from "new" to "current" to "old" ←
//
//	              Old         Current        New
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │ █
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │ █
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │ █ █
//	            █ █ █ █ │ █ █ █ █ █ █ █ █ │ █ █ █
//	            ↓ ↓ ↓ ↓                     ↑ ↑ ↑ ↑
//	            └─┴─┴─┴─────────────────────┴─┴─┴─┘
//	   Data gets copied from "old" to "new" when requested.
//
// Blobs get stored in blocks in the "new" group with an inverse
// exponential probability. This is done to reduce the probability of
// multiple block rotations close after each other, as this might put
// excessive pressure on the garbage collector. Because the placement
// distribution decreases rapidly, having more than three or four "new"
// blocks would be wasteful. Having fewer is also not recommended, as
// that increases the chance of placing objects that are used together
// inside the same block. This may cause 'tidal waves' of I/O whenever
// such data ends up in the "old" group at once.
//
// After initialization, there will be fewer blocks in the "current"
// group than configured, due to there simply being no data. This is
// compensated by adding more blocks to the "new" group. Unlike the
// regular blocks in this group, these will have a uniform placement
// distribution that is twice as high as normal. This is done to ensure
// the "current" blocks are randomly seeded to reduce 'tidal waves'
// later on.
//
// The number of blocks in the "old" group should not be too low, as
// this would cause this storage backend to become a FIFO instead of
// being LRU-like. Setting it too high is also not recommended, as this
// would increase redundancy in the data stored. The "current" group
// should likely be two or three times as large as the "old" group.
type OldCurrentNewLocationBlobMap struct {
	blockList             BlockList
	blockListGrowthPolicy BlockListGrowthPolicy
	errorLogger           util.ErrorLogger
	blockSizeBytes        int64
	desiredOldBlocksCount int
	desiredNewBlocksCount int

	// The number of blocks present in the underlying BlockList,
	// partitioned into "old", "current" and "new".
	oldBlocks     []oldBlockState
	currentBlocks int
	newBlocks     int

	// The total number of blocks that have been released, and the
	// total number that we should be releasing. These values are
	// used to force blocks with data corruption to be discarded.
	totalBlocksReleased     uint64
	totalBlocksToBeReleased atomic.Uint64

	// Counters to decide which block should store the next blob.
	allocationAttemptsRemaining int
	allocationBlockIndex        int

	lastRemovedOldBlockInsertionTime prometheus.Gauge
}

func unixTime() float64 {
	return time.Now().Sub(time.Unix(0, 0)).Seconds()
}

// NewOldCurrentNewLocationBlobMap creates a new instance of
// OldCurrentNewLocationBlobMap.
func NewOldCurrentNewLocationBlobMap(blockList BlockList, blockListGrowthPolicy BlockListGrowthPolicy, errorLogger util.ErrorLogger, storageType string, blockSizeBytes int64, oldBlocksCount, newBlocksCount, initialBlocksCount int) *OldCurrentNewLocationBlobMap {
	oldCurrentNewLocationBlobMapPrometheusMetrics.Do(func() {
		prometheus.MustRegister(oldCurrentNewLocationBlobMapLastRemovedOldBlockInsertionTime)
	})

	lbm := &OldCurrentNewLocationBlobMap{
		blockList:             blockList,
		blockListGrowthPolicy: blockListGrowthPolicy,
		errorLogger:           errorLogger,
		blockSizeBytes:        blockSizeBytes,
		desiredOldBlocksCount: oldBlocksCount,
		desiredNewBlocksCount: newBlocksCount,

		allocationBlockIndex: -1,

		lastRemovedOldBlockInsertionTime: oldCurrentNewLocationBlobMapLastRemovedOldBlockInsertionTime.WithLabelValues(storageType),
	}
	now := unixTime()
	lbm.lastRemovedOldBlockInsertionTime.Set(now)

	// Configure the layout based on the number of blocks that were
	// persisted and restored. Promote as many blocks as possible
	// from "old" to "new". Once that option is exhausted, promote
	// additional blocks from "old" to "current". It may be the case
	// that we're left with too many "old" blocks afterwards. Force
	// those to be released during the next Put() operation.
	initialOldBlocksCount := initialBlocksCount
	for initialOldBlocksCount > 0 && blockListGrowthPolicy.ShouldGrowNewBlocks(0, lbm.newBlocks) {
		initialOldBlocksCount--
		lbm.newBlocks++
	}
	for initialOldBlocksCount > 0 && blockListGrowthPolicy.ShouldGrowCurrentBlocks(lbm.currentBlocks) {
		initialOldBlocksCount--
		lbm.currentBlocks++
	}
	for i := 0; i < initialOldBlocksCount; i++ {
		lbm.oldBlocks = append(lbm.oldBlocks, oldBlockState{
			insertionTime: now,
		})
	}
	if len(lbm.oldBlocks) > lbm.desiredOldBlocksCount {
		lbm.totalBlocksToBeReleased.Store(uint64(len(lbm.oldBlocks) - lbm.desiredOldBlocksCount))
	}
	return lbm
}

var (
	_ LocationBlobMap        = (*OldCurrentNewLocationBlobMap)(nil)
	_ BlockReferenceResolver = (*OldCurrentNewLocationBlobMap)(nil)
)

// BlockReferenceToBlockIndex converts a BlockReference that contains a
// stable reference to a block to an integer index. The integer index
// corresponds to the current location of the block in the underlying
// BlockList.
func (lbm *OldCurrentNewLocationBlobMap) BlockReferenceToBlockIndex(blockReference BlockReference) (int, uint64, bool) {
	blockIndex, hashSeed, found := lbm.blockList.BlockReferenceToBlockIndex(blockReference)
	if !found {
		return 0, 0, false
	}
	if uint64(blockIndex) < lbm.totalBlocksToBeReleased.Load()-lbm.totalBlocksReleased {
		// We know data corruption exists for this block. We
		// just haven't been able to release this block yet,
		// because no calls to Put() were made in the meantime.
		//
		// Let's make these blocks unresolvable, so that we stop
		// serving corrupted data immediately.
		return 0, 0, false
	}
	return blockIndex, hashSeed, found
}

// BlockIndexToBlockReference converts an integer index of a block in
// the underlying BlockList to a BlockReference. The BlockReference is a
// stable referennce to this block that remains valid after locks are
// dropped.
func (lbm *OldCurrentNewLocationBlobMap) BlockIndexToBlockReference(blockIndex int) (BlockReference, uint64) {
	// It may be possible that the block index refers to a block for
	// which we know data corruption exists. This happens when data
	// corruption is reported during a read, while writes to that
	// same block are still taking place place.
	//
	// For now, let's just allow thes writes to complete. The
	// resulting blobs will, however, be unreadable.
	return lbm.blockList.BlockIndexToBlockReference(blockIndex)
}

// increaseTotalBlocksToBeReleased increases the value of
// lbm.totalBlocksToBeReleased. It uses atomic operations to increment
// this value, as this value needs to be adjusted from within
// DataIntegrityCallbacks. It is not easy to pick up locks from that
// context.
func (lbm *OldCurrentNewLocationBlobMap) increaseTotalBlocksToBeReleased(newValue uint64) uint64 {
	for {
		oldValue := lbm.totalBlocksToBeReleased.Load()
		if newValue <= oldValue {
			return 0
		}
		if lbm.totalBlocksToBeReleased.CompareAndSwap(oldValue, newValue) {
			return newValue - oldValue
		}
	}
}

// Get information about a blob based on its Location. A
// LocationBlobGetter is returned that can be used to fetch the blob's
// contents.
func (lbm *OldCurrentNewLocationBlobMap) Get(location Location) (LocationBlobGetter, bool) {
	return func(digest digest.Digest) buffer.Buffer {
		totalBlocksToBeReleased := lbm.totalBlocksReleased + uint64(location.BlockIndex) + 1
		return lbm.blockList.Get(location.BlockIndex, digest, location.OffsetBytes, location.SizeBytes, func(dataIsValid bool) {
			if !dataIsValid {
				if blocksReleased := lbm.increaseTotalBlocksToBeReleased(totalBlocksToBeReleased); blocksReleased > 0 {
					lbm.errorLogger.Log(status.Errorf(codes.Internal, "Releasing %d blocks due to a data integrity error", blocksReleased))
				}
			}
		})
	}, location.BlockIndex < len(lbm.oldBlocks)
}

// startAllocatingFromBlock resets the counters used to determine from
// which "new" block to allocate data. This function is called whenever
// the list of "new" blocks changes.
func (lbm *OldCurrentNewLocationBlobMap) startAllocatingFromBlock(i int) {
	lbm.allocationBlockIndex = i
	if i >= lbm.newBlocks-lbm.desiredNewBlocksCount {
		// One of the actual "new" blocks.
		lbm.allocationAttemptsRemaining = 1 << (lbm.newBlocks - i - 1)
	} else {
		// One of the "current" blocks, while still in the
		// initial phase where we populate all blocks.
		lbm.allocationAttemptsRemaining = 1 << lbm.desiredNewBlocksCount
	}
}

func (lbm *OldCurrentNewLocationBlobMap) popFront() {
	lbm.blockList.PopFront()
	lbm.totalBlocksReleased++
}

func (lbm *OldCurrentNewLocationBlobMap) removeOldestOldBlock() {
	lbm.lastRemovedOldBlockInsertionTime.Set(lbm.oldBlocks[0].insertionTime)
	lbm.oldBlocks = lbm.oldBlocks[1:]
}

func (lbm *OldCurrentNewLocationBlobMap) findBlockWithSpace(sizeBytes int64) (int, error) {
	// Filter requests that can never be satisfied. Not doing so
	// would cause us to get stuck in the final loop of this
	// function.
	if sizeBytes > lbm.blockSizeBytes {
		return 0, status.Errorf(
			codes.InvalidArgument,
			"Blob is %d bytes in size, while this backend is only capable of storing blobs of up to %d bytes in size",
			sizeBytes,
			lbm.blockSizeBytes)
	}

	// Remove blocks from our bookkeeping in which data corruption
	// has been observed. This cannot be done as part of the
	// DataIntegrityCallback, as it's impossible to pick up a write
	// lock from that context.
	totalBlocksToBeReleased := lbm.totalBlocksToBeReleased.Load()
	for lbm.totalBlocksReleased < totalBlocksToBeReleased {
		lbm.popFront()
		if len(lbm.oldBlocks) > 0 {
			lbm.removeOldestOldBlock()
		} else if lbm.currentBlocks > 0 {
			lbm.currentBlocks--
		} else {
			lbm.newBlocks--
			lbm.startAllocatingFromBlock(0)
		}
	}

	// When in the initial state or after removing blocks due to
	// data corruption, we should ensure that we still have a
	// sufficient number of "new" blocks from which we can allocate
	// data.
	for lbm.blockListGrowthPolicy.ShouldGrowNewBlocks(lbm.currentBlocks, lbm.newBlocks) {
		if err := lbm.blockList.PushBack(); err != nil {
			return 0, err
		}
		lbm.newBlocks++
	}

	// Move the first "new" block(s) to "current" whenever they no
	// longer have enough space to fit a blob. This ensures that the
	// final loop is always capable of finding one block with space.
	for !lbm.blockList.HasSpace(len(lbm.oldBlocks)+lbm.currentBlocks, sizeBytes) {
		if lbm.newBlocks > lbm.desiredNewBlocksCount {
			// This is still an excessive block from the
			// initialization phase. Just move a block from
			// "new" to "current".
			lbm.currentBlocks++
			lbm.newBlocks--
		} else {
			// The initialization phase is way behind us.
			// Create a new block, thereby causing one block
			// to be moved from "new" to "current", and one
			// block to be moved from "current" to "old".
			if err := lbm.blockList.PushBack(); err != nil {
				return 0, err
			}

			lbm.oldBlocks = append(lbm.oldBlocks, oldBlockState{
				insertionTime: unixTime(),
			})
			if len(lbm.oldBlocks) > lbm.desiredOldBlocksCount {
				lbm.popFront()
				lbm.removeOldestOldBlock()
				lbm.increaseTotalBlocksToBeReleased(lbm.totalBlocksReleased)
			}
		}
		lbm.startAllocatingFromBlock(0)
	}

	// Repeatedly attempt to allocate a blob within a "new" block.
	for {
		if lbm.allocationAttemptsRemaining > 0 {
			index := len(lbm.oldBlocks) + lbm.currentBlocks + lbm.allocationBlockIndex
			if lbm.blockList.HasSpace(index, sizeBytes) {
				lbm.allocationAttemptsRemaining--
				return index, nil
			}
		}
		lbm.startAllocatingFromBlock((lbm.allocationBlockIndex + 1) % lbm.newBlocks)
	}
}

// Put a new blob of a given size to storage.
func (lbm *OldCurrentNewLocationBlobMap) Put(sizeBytes int64) (LocationBlobPutWriter, error) {
	blockIndex, err := lbm.findBlockWithSpace(sizeBytes)
	if err != nil {
		return nil, err
	}

	// Convert the block index to an absolute value to account for
	// block rotations happening while the write takes place. It
	// gets converted back to a relative value later on.
	absoluteBlockIndex := lbm.totalBlocksReleased + uint64(blockIndex)
	putWriter := lbm.blockList.Put(blockIndex, sizeBytes)
	return func(b buffer.Buffer) LocationBlobPutFinalizer {
		putFinalizer := putWriter(b)
		return func() (Location, error) {
			offsetBytes, err := putFinalizer()
			if err != nil {
				return Location{}, err
			}

			// If data integrity errors occur, the write
			// takes very long to complete, or storage is
			// simply sized inadequately, the write won't
			// have any effect. Return an error, so that
			// clients retry.
			if absoluteBlockIndex < lbm.totalBlocksToBeReleased.Load() {
				return Location{}, status.Error(codes.Internal, "The block to which this blob was written, has already been released")
			}
			return Location{
				BlockIndex:  int(absoluteBlockIndex - lbm.totalBlocksReleased),
				OffsetBytes: offsetBytes,
				SizeBytes:   sizeBytes,
			}, nil
		}
	}, nil
}
