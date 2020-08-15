package local

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	localBlobAccessOldBlobRotationToNew = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "local_blob_access_old_blobs_rotated_to_new",
			Help:      "The number of blobs in old blocks, rotated to new blocks",
			Buckets:   append([]float64{0}, prometheus.ExponentialBuckets(1.0, 2.0, 16)...),
		},
		[]string{"name", "operation"})
)

// sharedBlock is a reference counted Block. Whereas Block can only be
// released once, sharedBlock has a pair of acquire() and release()
// functions. This type is used for being able to call Put() on a Block
// in such a way that the underlying Block does not disappear.
//
// The reference count stored by sharedBlock is not updated atomically.
// It can only be mutated safely by locking the containing
// localBlobAccess.
type sharedBlock struct {
	b        Block
	refcount uint64
}

func newSharedBlock(b Block) *sharedBlock {
	return &sharedBlock{
		b:        b,
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
		sb.b.Release()
	}
}

// deadBlock is a placeholder implementation of Block. It is used to
// initialize all "old" blocks of LocalBlobAccess. This is done to
// ensure that any attempts to access or release these blocks don't lead
// to nil pointer dereferences.
type deadBlock struct{}

func (db deadBlock) Get(digest digest.Digest, offset int64, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	panic("Attempted to read blob from dead block")
}

func (db deadBlock) Put(offset int64, b buffer.Buffer) error {
	panic("Attempted to write blob into dead block")
}

func (db deadBlock) Release() {}

type oldBlock struct {
	block         *sharedBlock
	insertionTime float64
}

type newBlock struct {
	block  *sharedBlock
	offset int64
}

type localBlobAccess struct {
	sectorSizeBytes       int
	blockSectorCount      int64
	blockAllocator        BlockAllocator
	errorLogger           util.ErrorLogger
	digestKeyFormat       digest.KeyFormat
	desiredNewBlocksCount int

	lock                        sync.Mutex
	refreshLock                 sync.Mutex
	digestLocationMap           DigestLocationMap
	oldBlocks                   []oldBlock
	currentBlocks               []*sharedBlock
	newBlocks                   []newBlock
	oldestBlockID               int
	locationValidator           LocationValidator
	allocationBlockIndex        int
	allocationAttemptsRemaining int

	lastRemovedOldBlockInsertionTime prometheus.Gauge
	oldBlobRotationToNewGet          prometheus.Observer
	oldBlobRotationToNewFindMissing  prometheus.Observer
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
func NewLocalBlobAccess(digestLocationMap DigestLocationMap, blockAllocator BlockAllocator, errorLogger util.ErrorLogger, digestKeyFormat digest.KeyFormat, name string, sectorSizeBytes int, blockSectorCount int64, oldBlocksCount int, currentBlocksCount int, newBlocksCount int) (blobstore.BlobAccess, error) {
	localBlobAccessPrometheusMetrics.Do(func() {
		prometheus.MustRegister(localBlobAccessLastRemovedOldBlockInsertionTime)
		prometheus.MustRegister(localBlobAccessOldBlobRotationToNew)
	})

	ba := &localBlobAccess{
		sectorSizeBytes:  sectorSizeBytes,
		blockSectorCount: blockSectorCount,
		blockAllocator:   blockAllocator,
		errorLogger:      errorLogger,
		digestKeyFormat:  digestKeyFormat,

		digestLocationMap: digestLocationMap,
		oldestBlockID:     1,
		locationValidator: LocationValidator{
			OldestValidBlockID: oldBlocksCount + 1,
			NewestValidBlockID: oldBlocksCount + currentBlocksCount + newBlocksCount,
		},
		desiredNewBlocksCount: newBlocksCount,

		lastRemovedOldBlockInsertionTime: localBlobAccessLastRemovedOldBlockInsertionTime.WithLabelValues(name),
		oldBlobRotationToNewGet:          localBlobAccessOldBlobRotationToNew.WithLabelValues(name, "Get"),
		oldBlobRotationToNewFindMissing:  localBlobAccessOldBlobRotationToNew.WithLabelValues(name, "FindMissing"),
	}

	// Insert placeholders for the initial set of "old" blocks.
	now := unixTime()
	ba.lastRemovedOldBlockInsertionTime.Set(now)
	for i := 0; i < oldBlocksCount; i++ {
		ba.oldBlocks = append(ba.oldBlocks, oldBlock{
			block:         newSharedBlock(deadBlock{}),
			insertionTime: now,
		})
	}

	// Allocate initial set of "new" blocks.
	for i := 0; i < currentBlocksCount+newBlocksCount; i++ {
		block, err := blockAllocator.NewBlock()
		if err != nil {
			for _, newBlock := range ba.newBlocks {
				newBlock.block.release()
			}
			return nil, err
		}
		ba.newBlocks = append(ba.newBlocks, newBlock{
			block: newSharedBlock(block),
		})
	}
	ba.startAllocatingFromBlock(0)
	return ba, nil
}

// getBlock returns the block associated with a numerical block ID.
func (ba *localBlobAccess) getBlock(blockID int) (block *sharedBlock, isOld bool) {
	blockID -= ba.oldestBlockID
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

func (ba *localBlobAccess) allocateSpace(sizeBytes int64) (*sharedBlock, Location, error) {
	// Determine the number of sectors needed to store the object.
	// TODO: This can be wasteful for storing small objects with
	// large sector sizes. Should we add logic for packing small
	// objects together into a single sector?
	sectors := (sizeBytes + int64(ba.sectorSizeBytes) - 1) / int64(ba.sectorSizeBytes)

	// Move the first "new" block(s) to "current" whenever they no
	// longer have enough space to fit a blob. This ensures that the
	// next loop is always capable of finding some block with space.
	for ba.blockSectorCount-ba.newBlocks[0].offset < sectors {
		if len(ba.newBlocks) > ba.desiredNewBlocksCount {
			// This is still an excessive block from the
			// initialization phase.
			ba.currentBlocks = append(ba.currentBlocks, ba.newBlocks[0].block)
			ba.newBlocks = append([]newBlock{}, ba.newBlocks[1:]...)
		} else {
			// The initialization phase is way behind us.
			block, err := ba.blockAllocator.NewBlock()
			if err != nil {
				return nil, Location{}, err
			}
			ba.lastRemovedOldBlockInsertionTime.Set(ba.oldBlocks[0].insertionTime)
			ba.oldBlocks[0].block.release()
			ba.oldBlocks = append(append([]oldBlock{}, ba.oldBlocks[1:]...), oldBlock{
				block:         ba.currentBlocks[0],
				insertionTime: unixTime(),
			})
			ba.currentBlocks = append(append([]*sharedBlock{}, ba.currentBlocks[1:]...), ba.newBlocks[0].block)
			ba.newBlocks = append(append([]newBlock{}, ba.newBlocks[1:]...), newBlock{
				block: newSharedBlock(block),
			})
			ba.oldestBlockID++
			if ba.locationValidator.OldestValidBlockID < ba.oldestBlockID {
				ba.locationValidator.OldestValidBlockID = ba.oldestBlockID
			}
			ba.locationValidator.NewestValidBlockID++
		}
		ba.startAllocatingFromBlock(0)
	}

	// Repeatedly attempt to allocate a blob within a "new" block.
	for {
		if ba.allocationAttemptsRemaining > 0 {
			newBlock := &ba.newBlocks[ba.allocationBlockIndex]
			if offset := newBlock.offset; ba.blockSectorCount-offset >= sectors {
				ba.allocationAttemptsRemaining--
				newBlock.offset += sectors
				return newBlock.block, Location{
					BlockID: ba.oldestBlockID +
						len(ba.oldBlocks) +
						len(ba.currentBlocks) +
						ba.allocationBlockIndex,
					OffsetBytes: offset * int64(ba.sectorSizeBytes),
					SizeBytes:   sizeBytes,
				}, nil
			}
		}
		ba.startAllocatingFromBlock((ba.allocationBlockIndex + 1) % len(ba.newBlocks))
	}
}

func (ba *localBlobAccess) discardCorruptedBlocks(blockID int) {
	ba.lock.Lock()
	oldestValidBlockID := ba.locationValidator.OldestValidBlockID
	if oldestValidBlockID <= blockID {
		// Prevent DigestLocationMap lookups resulting into
		// access to blocks with data corruption.
		ba.locationValidator.OldestValidBlockID = blockID + 1

		// If any "new" blocks are affected, mark them fully
		// used, so that no new blobs end up being placed in
		// them.
		oldestNewBlockID := ba.oldestBlockID + len(ba.oldBlocks) + len(ba.currentBlocks)
		for i := range ba.newBlocks {
			if oldestNewBlockID+i > blockID {
				break
			}
			ba.newBlocks[i].offset = ba.blockSectorCount
		}
	}
	ba.lock.Unlock()

	if oldestValidBlockID <= blockID {
		ba.errorLogger.Log(status.Errorf(codes.Internal, "Discarded blocks %d to %d due to a data integrity error", oldestValidBlockID, blockID))
	}
}

func (ba *localBlobAccess) getDataIntegrityCallback(blockID int) buffer.DataIntegrityCallback {
	return func(dataIsValid bool) {
		if !dataIsValid {
			// Data corruption was detected in one of the
			// blobs. Though we could discard individual
			// blobs selectively, this may lead to many
			// failing requests if data corruption is
			// widespread.
			//
			// Go ahead and effectively discard all of the
			// blocks up to and including the one containing
			// the data corruption. This keeps the number of
			// request failures reduced to a minimum.
			//
			// This needs to happen in its own goroutine, as
			// the DataIntegrityCallback can be called in
			// places where ba.lock is held.
			go ba.discardCorruptedBlocks(blockID)
		}
	}
}

func (ba *localBlobAccess) getCompactDigest(digest digest.Digest) CompactDigest {
	return NewCompactDigest(digest.GetKey(ba.digestKeyFormat))
}

func (ba *localBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	// Look up the blob in the offset store.
	compactDigest := ba.getCompactDigest(digest)
	ba.lock.Lock()
	readLocation, err := ba.digestLocationMap.Get(compactDigest, &ba.locationValidator)
	if err != nil {
		ba.lock.Unlock()
		return buffer.NewBufferFromError(err)
	}

	readBlock, isOld := ba.getBlock(readLocation.BlockID)
	b := readBlock.b.Get(digest, readLocation.OffsetBytes, readLocation.SizeBytes, ba.getDataIntegrityCallback(readLocation.BlockID))
	if !isOld {
		// Blob was found in a "new" or "current" block.
		ba.lock.Unlock()
		return b
	}

	// Blob was found, but it is stored in an "old" block. Allocate
	// new space to copy the blob on the fly.
	//
	// TODO: Instead of copying data on the fly, should this be done
	// immediately, so that we can prevent potential duplication by
	// picking up the refresh lock?
	writeBlock, writeLocation, err := ba.allocateSpace(readLocation.SizeBytes)
	if err != nil {
		ba.lock.Unlock()
		b.Discard()
		return buffer.NewBufferFromError(err)
	}
	writeBlock.acquire()
	ba.lock.Unlock()

	// Copy the object while it's been returned. Block until copying
	// has finished to apply back-pressure.
	b1, b2 := b.CloneStream()
	b1, t := buffer.WithBackgroundTask(b1)
	go func() {
		err := writeBlock.b.Put(writeLocation.OffsetBytes, b2)

		ba.lock.Lock()
		writeBlock.release()
		if err == nil {
			err = ba.digestLocationMap.Put(compactDigest, &ba.locationValidator, writeLocation)
			ba.oldBlobRotationToNewGet.Observe(float64(1))
		}
		ba.lock.Unlock()

		t.Finish(err)
	}()
	return b1
}

func (ba *localBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	sizeBytes, err := b.GetSizeBytes()
	if err != nil {
		b.Discard()
		return err
	}
	if blockSizeBytes := int64(ba.sectorSizeBytes) * ba.blockSectorCount; sizeBytes > blockSizeBytes {
		return status.Errorf(
			codes.InvalidArgument,
			"Blob is %d bytes in size, while this backend is only capable of storing blobs of up to %d bytes in size",
			sizeBytes,
			blockSizeBytes)
	}
	compactDigest := ba.getCompactDigest(digest)

	ba.lock.Lock()
	defer ba.lock.Unlock()

	// Allocate space to store the object.
	block, location, err := ba.allocateSpace(sizeBytes)
	if err != nil {
		return err
	}

	// Copy the the object into storage. This needs to acquire the
	// block to prevent it from disappearing during transfer.
	block.acquire()
	ba.lock.Unlock()
	err = block.b.Put(location.OffsetBytes, b)
	ba.lock.Lock()
	block.release()
	if err != nil {
		return err
	}

	// Upon successful completion, expose the object in storage.
	return ba.digestLocationMap.Put(compactDigest, &ba.locationValidator, location)
}

type oldBlob struct {
	digest        digest.Digest
	compactDigest CompactDigest
}

func (ba *localBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Convert all digests to their internal representation.
	compactDigests := make([]CompactDigest, 0, digests.Length())
	for _, blobDigest := range digests.Items() {
		compactDigests = append(compactDigests, ba.getCompactDigest(blobDigest))
	}

	ba.lock.Lock()
	defer ba.lock.Unlock()

	var old []oldBlob
	missing := digest.NewSetBuilder()
	for i, blobDigest := range digests.Items() {
		compactDigest := compactDigests[i]
		if readLocation, err := ba.digestLocationMap.Get(compactDigest, &ba.locationValidator); err == nil {
			if _, isOld := ba.getBlock(readLocation.BlockID); isOld {
				// Blob is present, but it must be
				// refreshed for it to remain in storage.
				old = append(old, oldBlob{
					digest:        blobDigest,
					compactDigest: compactDigest,
				})
			}
		} else if status.Code(err) == codes.NotFound {
			// Blob is absent.
			missing.Add(blobDigest)
		} else {
			return digest.EmptySet, err
		}
	}
	if len(old) == 0 {
		return missing.Build(), nil
	}

	// One or more blobs need to be refreshed.
	//
	// We should prevent concurrent FindMissing() calls from
	// refreshing the same blobs, as that would cause data to be
	// duplicated and load to increase significantly. Pick up the
	// refresh lock to ensure bandwidth of refreshing is limited to
	// one thread.
	ba.lock.Unlock()
	ba.refreshLock.Lock()
	defer ba.refreshLock.Unlock()
	ba.lock.Lock()

	blobsRefreshedSuccessfully := 0
	for _, oldBlob := range old {
		if readLocation, err := ba.digestLocationMap.Get(oldBlob.compactDigest, &ba.locationValidator); err == nil {
			if readBlock, isOld := ba.getBlock(readLocation.BlockID); isOld {
				// Blob is present and still old.
				// Allocate space for a copy.
				b := readBlock.b.Get(oldBlob.digest, readLocation.OffsetBytes, readLocation.SizeBytes, ba.getDataIntegrityCallback(readLocation.BlockID))
				writeBlock, writeLocation, err := ba.allocateSpace(readLocation.SizeBytes)
				if err != nil {
					b.Discard()
					return digest.EmptySet, err
				}

				// Copy the data while unlocked, so that
				// concurrent requests for non-old data
				// continue to be serviced.
				writeBlock.acquire()
				ba.lock.Unlock()
				err = writeBlock.b.Put(writeLocation.OffsetBytes, b)
				ba.lock.Lock()
				writeBlock.release()
				if err != nil {
					return digest.EmptySet, err
				}

				if err := ba.digestLocationMap.Put(oldBlob.compactDigest, &ba.locationValidator, writeLocation); err != nil {
					return digest.EmptySet, err
				}
				blobsRefreshedSuccessfully++
			}
		} else if status.Code(err) == codes.NotFound {
			// Blob disappeared after the first iteration.
			missing.Add(oldBlob.digest)
		} else {
			return digest.EmptySet, err
		}
		ba.oldBlobRotationToNewFindMissing.Observe(float64(blobsRefreshedSuccessfully))
	}
	return missing.Build(), nil
}
