package local

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
)

var (
	localBlobAccessPrometheusMetrics sync.Once

	localBlobAccessLastRemovedOldBlockInsertionTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "local_blob_access_last_removed_old_block_insertion_time_seconds",
			Help:      "Time at which the last removed block was inserted into the \"old\" queue, which is an indicator for the worst-case blob retention time",
		},
		[]string{"name"})
)

type oldBlock struct {
	block         Block
	insertionTime float64
}

type newBlock struct {
	block  Block
	offset int64
}

type localBlobAccess struct {
	blockSize             int64
	blockAllocator        BlockAllocator
	desiredNewBlocksCount int

	lock                        sync.Mutex
	digestLocationMap           DigestLocationMap
	oldBlocks                   []oldBlock
	currentBlocks               []Block
	newBlocks                   []newBlock
	locationValidator           LocationValidator
	allocationBlockIndex        int
	allocationAttemptsRemaining int

	lastRemovedOldBlockInsertionTime prometheus.Gauge
}

func unixTime() float64 {
	return time.Now().Sub(time.Unix(0, 0)).Seconds()
}

// NewLocalBlobAccess creates a caching storage backend that stores data
// on the local system (e.g., on disk or in memory). This backend works
// by storing blobs in blocks. Blobs cannot span multiple blocks,
// meaning that blocks generally need to be large in size (gigabytes).
// The number of blocks may be relatively low. For example, for a 512
// GiB cache, it is acceptable to create 32 blocks of 16 GiB in size.
//
// Blocks are partitioned into three groups based on their creation
// time, named "old", "current" and "new". Blobs provided to Put() will
// always be stored in a block in the "new" group. When the oldest block
// in the "new" group becomes full, it is moved to the "current" group.
// This causes the oldest block in the "current" group to be displaced
// to the "old" group. The oldest block in the "old" group is discarded.
//
// The difference between the "current" group and the "old" group is in
// how data gets treated when requested through Get() and FindMissing().
// Data in the "old" group is at risk of being removed in the nearby
// future, which is why it is copied into the "new" group when
// requested. Data in the "current" group is assumed to remain present
// for the time being, which is why it is left in place.
//
// Below is an illustration of how the blocks of data may be laid out at
// a given point in time. Every column of █ characters corresponds to a
// single block. The number of characters indicates the amount of data
// stored within.
//
//     ← Over time, blocks move from "new" to "current" to "old" ←
//
//                   Old         Current        New
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │ █
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │ █
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │ █ █
//                 █ █ █ █ │ █ █ █ █ █ █ █ █ │ █ █ █
//                 ↓ ↓ ↓ ↓                     ↑ ↑ ↑ ↑
//                 └─┴─┴─┴─────────────────────┴─┴─┴─┘
//        Data gets copied from "old" to "new" when requested.
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
func NewLocalBlobAccess(digestLocationMap DigestLocationMap, blockAllocator BlockAllocator, name string, blockSize int64, oldBlocksCount int, currentBlocksCount int, newBlocksCount int) blobstore.BlobAccess {
	localBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(localBlobAccessLastRemovedOldBlockInsertionTime)
	})

	ba := &localBlobAccess{
		blockSize:      blockSize,
		blockAllocator: blockAllocator,

		digestLocationMap: digestLocationMap,
		locationValidator: LocationValidator{
			OldestBlockID: 1,
			NewestBlockID: oldBlocksCount + currentBlocksCount + newBlocksCount,
		},
		desiredNewBlocksCount: newBlocksCount,

		lastRemovedOldBlockInsertionTime: localBlobAccessLastRemovedOldBlockInsertionTime.WithLabelValues(name),
	}

	// Insert placeholders for the initial set of "old" blocks.
	now := unixTime()
	ba.lastRemovedOldBlockInsertionTime.Set(now)
	for i := 0; i < oldBlocksCount; i++ {
		ba.oldBlocks = append(ba.oldBlocks, oldBlock{
			insertionTime: now,
		})
	}

	// Allocate initial set of "new" blocks.
	for i := 0; i < currentBlocksCount+newBlocksCount; i++ {
		ba.newBlocks = append(ba.newBlocks, newBlock{
			block: blockAllocator.NewBlock(),
		})
	}
	ba.startAllocatingFromBlock(0)
	return ba
}

// getBlock returns the block associated with a numerical block ID.
func (ba *localBlobAccess) getBlock(blockID int) (block Block, isOld bool) {
	blockID -= ba.locationValidator.OldestBlockID
	if blockID < len(ba.oldBlocks) {
		return ba.oldBlocks[blockID].block, true
	}
	blockID -= len(ba.oldBlocks)
	if blockID < len(ba.currentBlocks) {
		return ba.currentBlocks[blockID], false
	}
	blockID -= len(ba.currentBlocks)
	return ba.newBlocks[blockID].block, false
}

// startAllocatingFromBlock resets the counters used to determine from
// which "new" block to allocate data. This function is called whenever
// the list of "new" blocks changes.
func (ba *localBlobAccess) startAllocatingFromBlock(i int) {
	ba.allocationBlockIndex = i
	if i >= len(ba.newBlocks)-ba.desiredNewBlocksCount {
		// One of the actual "new" blocks.
		ba.allocationAttemptsRemaining = 1 << (len(ba.newBlocks) - i - 1)
	} else {
		// One of the "current" blocks, while still in the
		// initial phase where we populate all blocks.
		ba.allocationAttemptsRemaining = 1 << ba.desiredNewBlocksCount
	}
}

func (ba *localBlobAccess) allocateSpace(sizeBytes int64) (Block, Location) {
	// Move the first "new" block(s) to "current" whenever they no
	// longer have enough space to fit a blob. This ensures that the
	// next loop is always capable of finding some block with space.
	for ba.blockSize-ba.newBlocks[0].offset < sizeBytes {
		if len(ba.newBlocks) > ba.desiredNewBlocksCount {
			// This is still an excessive block from the
			// initialization phase.
			ba.currentBlocks = append(ba.currentBlocks, ba.newBlocks[0].block)
			ba.newBlocks = append([]newBlock{}, ba.newBlocks[1:]...)
		} else {
			// The initialization phase is way behind us.
			ba.lastRemovedOldBlockInsertionTime.Set(ba.oldBlocks[0].insertionTime)
			ba.oldBlocks = append(append([]oldBlock{}, ba.oldBlocks[1:]...), oldBlock{
				block:         ba.currentBlocks[0],
				insertionTime: unixTime(),
			})
			ba.currentBlocks = append(append([]Block{}, ba.currentBlocks[1:]...), ba.newBlocks[0].block)
			ba.newBlocks = append(append([]newBlock{}, ba.newBlocks[1:]...), newBlock{
				block: ba.blockAllocator.NewBlock(),
			})
			ba.locationValidator.OldestBlockID++
			ba.locationValidator.NewestBlockID++
		}
		ba.startAllocatingFromBlock(0)
	}

	// Repeatedly attempt to allocate a blob within a "new" block.
	for {
		if ba.allocationAttemptsRemaining > 0 {
			newBlock := &ba.newBlocks[ba.allocationBlockIndex]
			if offset := newBlock.offset; ba.blockSize-offset >= sizeBytes {
				ba.allocationAttemptsRemaining--
				newBlock.offset += sizeBytes
				return newBlock.block, Location{
					BlockID: ba.locationValidator.OldestBlockID +
						len(ba.oldBlocks) +
						len(ba.currentBlocks) +
						ba.allocationBlockIndex,
					Offset:    offset,
					SizeBytes: sizeBytes,
				}
			}
		}
		ba.startAllocatingFromBlock((ba.allocationBlockIndex + 1) % len(ba.newBlocks))
	}
}

func (ba *localBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	ctx, span := trace.StartSpan(ctx, "blobstore.LocalBlobAccess.Get")
	defer span.End()

	// Look up the blob in the offset store.
	ba.lock.Lock()
	readLocation, err := ba.digestLocationMap.Get(digest, &ba.locationValidator)
	if err != nil {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}

	readBlock, isOld := ba.getBlock(readLocation.BlockID)
	if !isOld {
		// Blob was found in a "new" or "current" block.
		ba.lock.Unlock()
		return readBlock.Get(readLocation.Offset, readLocation.SizeBytes)
	}

	// Blob was found, but it is stored in an "old" block. Allocate
	// new space and copy the blob on the fly. Do require Get() to
	// block until copying has finished to apply back-pressure.
	writeBlock, writeLocation := ba.allocateSpace(readLocation.SizeBytes)
	ba.lock.Unlock()
	b1, b2 := readBlock.Get(readLocation.Offset, readLocation.SizeBytes).CloneStream()
	b1, t := buffer.WithBackgroundTask(b1)
	go func() {
		if err := writeBlock.Put(writeLocation.Offset, b2); err != nil {
			t.Finish(err)
			return
		}
		ba.lock.Lock()
		err := ba.digestLocationMap.Put(digest, &ba.locationValidator, writeLocation)
		ba.lock.Unlock()
		t.Finish(err)
	}()
	return b1
}

func (ba *localBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	ctx, span := trace.StartSpan(ctx, "blobstore.LocalBlobAccess.Put")
	defer span.End()

	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}
	if sizeBytes > ba.blockSize {
		return status.Errorf(
			codes.InvalidArgument,
			"Blob is %d bytes in size, while this backend is only capable of storing blobs of up to %d bytes in size",
			sizeBytes,
			ba.blockSize)
	}

	ba.lock.Lock()
	block, location := ba.allocateSpace(sizeBytes)
	ba.lock.Unlock()

	if err := block.Put(location.Offset, b); err != nil {
		return err
	}

	ba.lock.Lock()
	err = ba.digestLocationMap.Put(digest, &ba.locationValidator, location)
	ba.lock.Unlock()
	return err
}

type blobRefresh struct {
	digest        *util.Digest
	sizeBytes     int64
	readBlock     Block
	readOffset    int64
	writeBlock    Block
	writeLocation Location
}

func (ba *localBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ctx, span := trace.StartSpan(ctx, "blobstore.LocalBlobAccess.FindMissing")
	defer span.End()

	// Scan the offset store to determine which blobs are present.
	ba.lock.Lock()
	var missing []*util.Digest
	var blobRefreshes []blobRefresh
	for _, digest := range digests {
		readLocation, err := ba.digestLocationMap.Get(digest, &ba.locationValidator)
		if err == nil {
			if readBlock, isOld := ba.getBlock(readLocation.BlockID); isOld {
				// Blob is present, but it is stored in an "old"
				// block. Prepare to copy it to a "new" block.
				writeBlock, writeLocation := ba.allocateSpace(readLocation.SizeBytes)
				blobRefreshes = append(blobRefreshes, blobRefresh{
					digest:        digest,
					readBlock:     readBlock,
					readOffset:    readLocation.Offset,
					writeBlock:    writeBlock,
					writeLocation: writeLocation,
				})
			}
		} else if status.Code(err) == codes.NotFound {
			missing = append(missing, digest)
		} else {
			ba.lock.Unlock()
			return nil, err
		}
	}
	ba.lock.Unlock()

	// Copy all blobs from "old" to "new".
	var err error
	blobsRefreshedSuccessfully := 0
	for _, br := range blobRefreshes {
		err = br.writeBlock.Put(
			br.writeLocation.Offset,
			br.readBlock.Get(br.readOffset, br.writeLocation.SizeBytes))
		if err != nil {
			break
		}
		blobsRefreshedSuccessfully++
	}

	// Adjust the offset store to let all blobs point to their new
	// locations in the "new" blocks.
	if blobsRefreshedSuccessfully > 0 {
		ba.lock.Lock()
		for _, br := range blobRefreshes[:blobsRefreshedSuccessfully] {
			putErr := ba.digestLocationMap.Put(br.digest, &ba.locationValidator, br.writeLocation)
			if err == nil {
				err = putErr
			}
		}
		ba.lock.Unlock()
	}
	return missing, err
}
