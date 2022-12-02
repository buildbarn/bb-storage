package local

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	blockDeviceBackedBlockAllocatorPrometheusMetrics sync.Once

	blockDeviceBackedBlockAllocatorAllocations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "block_device_backed_block_allocator_allocations_total",
			Help:      "Number of times blocks managed by BlockDeviceBackedBlockAllocator were allocated",
		},
		[]string{"storage_type"})
	blockDeviceBackedBlockAllocatorReleases = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "block_device_backed_block_allocator_releases_total",
			Help:      "Number of times blocks managed by BlockDeviceBackedBlockAllocator were released",
		},
		[]string{"storage_type"})
	blockDeviceBackedBlockAllocatorGetsStarted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "block_device_backed_block_allocator_gets_started_total",
			Help:      "Number of Get() operations BlockDeviceBackedBlockAllocator that were started",
		},
		[]string{"storage_type"})
	blockDeviceBackedBlockAllocatorGetsCompleted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "block_device_backed_block_allocator_gets_completed_total",
			Help:      "Number of Get() operations BlockDeviceBackedBlockAllocator that were completed",
		},
		[]string{"storage_type"})
)

type blockDeviceBackedBlockAllocator struct {
	blockDevice       blockdevice.BlockDevice
	readBufferFactory blobstore.ReadBufferFactory
	sectorSizeBytes   int
	blockSectorCount  int64

	blockAllocatorAllocations   prometheus.Counter
	blockAllocatorReleases      prometheus.Counter
	blockAllocatorGetsStarted   prometheus.Counter
	blockAllocatorGetsCompleted prometheus.Counter

	lock        sync.Mutex
	freeOffsets []int64
}

// NewBlockDeviceBackedBlockAllocator implements a BlockAllocator that
// can be used by implementations of BlockList to store data. Blocks
// created by this allocator are backed by a single BlockDevice. The
// BlockDevice is partitioned into equally sized blocks that are stored
// consecutively.
//
// Blocks are initially handed out by increasing offset. Later on, the
// least recently released blocks are reused. This adds wear leveling to
// the system.
//
// This implementation also ensures that writes against underlying
// storage are all performed at sector boundaries and sizes. This
// ensures that no unnecessary reads are performed.
func NewBlockDeviceBackedBlockAllocator(blockDevice blockdevice.BlockDevice, readBufferFactory blobstore.ReadBufferFactory, sectorSizeBytes int, blockSectorCount int64, blockCount int, storageType string) BlockAllocator {
	blockDeviceBackedBlockAllocatorPrometheusMetrics.Do(func() {
		prometheus.MustRegister(blockDeviceBackedBlockAllocatorAllocations)
		prometheus.MustRegister(blockDeviceBackedBlockAllocatorReleases)

		prometheus.MustRegister(blockDeviceBackedBlockAllocatorGetsStarted)
		prometheus.MustRegister(blockDeviceBackedBlockAllocatorGetsCompleted)
	})

	pa := &blockDeviceBackedBlockAllocator{
		blockDevice:       blockDevice,
		readBufferFactory: readBufferFactory,
		sectorSizeBytes:   sectorSizeBytes,
		blockSectorCount:  blockSectorCount,

		blockAllocatorAllocations:   blockDeviceBackedBlockAllocatorAllocations.WithLabelValues(storageType),
		blockAllocatorReleases:      blockDeviceBackedBlockAllocatorReleases.WithLabelValues(storageType),
		blockAllocatorGetsStarted:   blockDeviceBackedBlockAllocatorGetsStarted.WithLabelValues(storageType),
		blockAllocatorGetsCompleted: blockDeviceBackedBlockAllocatorGetsCompleted.WithLabelValues(storageType),
	}
	for i := 0; i < blockCount; i++ {
		pa.freeOffsets = append(pa.freeOffsets, int64(i)*blockSectorCount)
	}
	return pa
}

func (pa *blockDeviceBackedBlockAllocator) newBlockObject(deviceOffsetSectors, writeOffsetSectors int64) Block {
	pa.blockAllocatorAllocations.Inc()
	pb := &blockDeviceBackedBlock{
		blockAllocator:      pa,
		deviceOffsetSectors: deviceOffsetSectors,
		writeOffsetSectors:  writeOffsetSectors,
	}
	pb.usecount.Store(1)
	return pb
}

func (pa *blockDeviceBackedBlockAllocator) getBlockLocationMessage(deviceOffsetSectors int64) *pb.BlockLocation {
	return &pb.BlockLocation{
		OffsetBytes: deviceOffsetSectors * int64(pa.sectorSizeBytes),
		SizeBytes:   pa.blockSectorCount * int64(pa.sectorSizeBytes),
	}
}

func (pa *blockDeviceBackedBlockAllocator) NewBlock() (Block, *pb.BlockLocation, error) {
	pa.lock.Lock()
	defer pa.lock.Unlock()

	if len(pa.freeOffsets) == 0 {
		return nil, nil, status.Error(codes.Unavailable, "No unused blocks available")
	}
	deviceOffsetSectors := pa.freeOffsets[0]
	pa.freeOffsets = pa.freeOffsets[1:]
	return pa.newBlockObject(deviceOffsetSectors, 0), pa.getBlockLocationMessage(deviceOffsetSectors), nil
}

func (pa *blockDeviceBackedBlockAllocator) NewBlockAtLocation(location *pb.BlockLocation, writeOffsetBytes int64) (Block, bool) {
	pa.lock.Lock()
	defer pa.lock.Unlock()

	for i, deviceOffsetSectors := range pa.freeOffsets {
		if proto.Equal(pa.getBlockLocationMessage(deviceOffsetSectors), location) {
			pa.freeOffsets[i] = pa.freeOffsets[len(pa.freeOffsets)-1]
			pa.freeOffsets = pa.freeOffsets[:len(pa.freeOffsets)-1]
			return pa.newBlockObject(
				deviceOffsetSectors,
				(writeOffsetBytes+int64(pa.sectorSizeBytes)-1)/int64(pa.sectorSizeBytes),
			), true
		}
	}
	return nil, false
}

// sharedSector contains the bookkeeping of a single sector of storage
// that stores data belonging to more than one object. Calls to
// WriteAt() against such a sector must be synchronized, so that
// subsequent calls don't erase data that was written previously.
type sharedSector struct {
	writeOffsetBytes int

	lock sync.Mutex
	data []byte
}

type blockDeviceBackedBlock struct {
	usecount            atomic.Int64
	blockAllocator      *blockDeviceBackedBlockAllocator
	deviceOffsetSectors int64
	writeOffsetSectors  int64
	sharedSector        *sharedSector
}

func (pb *blockDeviceBackedBlock) Release() {
	if c := pb.usecount.Add(-1); c < 0 {
		panic(fmt.Sprintf("Release(): Block has invalid reference count %d", c))
	} else if c == 0 {
		// Block has no remaining consumers. Allow the region in
		// storage to be reused for new data.
		pa := pb.blockAllocator
		pa.lock.Lock()
		pa.freeOffsets = append(pa.freeOffsets, pb.deviceOffsetSectors)
		pa.lock.Unlock()
		pa.blockAllocatorReleases.Inc()
	}
}

func (pb *blockDeviceBackedBlock) Get(digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	if c := pb.usecount.Add(1); c <= 1 {
		panic(fmt.Sprintf("Get(): Block has invalid reference count %d", c))
	}
	pb.blockAllocator.blockAllocatorGetsStarted.Inc()

	return pb.blockAllocator.readBufferFactory.NewBufferFromReaderAt(
		digest,
		&blockDeviceBackedBlockReader{
			SectionReader: *io.NewSectionReader(
				pb.blockAllocator.blockDevice,
				pb.deviceOffsetSectors*int64(pb.blockAllocator.sectorSizeBytes)+offsetBytes,
				sizeBytes),
			block: pb,
		},
		sizeBytes,
		dataIntegrityCallback)
}

func (pb *blockDeviceBackedBlock) HasSpace(sizeBytes int64) bool {
	pa := pb.blockAllocator
	remainingSizeBytes := (pa.blockSectorCount - pb.writeOffsetSectors) * int64(pa.sectorSizeBytes)
	if pb.sharedSector != nil {
		// Don't allow overwriting the leading space of the
		// first sector that has already been handed out to the
		// previous object.
		remainingSizeBytes -= int64(pb.sharedSector.writeOffsetBytes)
	}
	return remainingSizeBytes >= sizeBytes
}

func (pb *blockDeviceBackedBlock) Put(sizeBytes int64) BlockPutWriter {
	if c := pb.usecount.Add(1); c <= 1 {
		panic(fmt.Sprintf("Put(): Block has invalid reference count %d", c))
	}

	// Construct writer.
	pa := pb.blockAllocator
	w := &blockDeviceBackedBlockWriter{
		blockAllocator: pa,
		offsetSectors:  pb.deviceOffsetSectors + pb.writeOffsetSectors,
		firstSector:    pb.sharedSector,
	}

	// Determine at which offset within the block the object is
	// going to be placed.
	writeOffsetBytes := pb.writeOffsetSectors * int64(pa.sectorSizeBytes)
	if firstSector := pb.sharedSector; firstSector != nil {
		writeOffsetBytes += int64(firstSector.writeOffsetBytes)
		w.firstSectorOffsetBytes = firstSector.writeOffsetBytes
	}

	// Allocate the desired number of sectors.
	endOffsetBytes := int64(w.firstSectorOffsetBytes) + sizeBytes
	sectorCount := endOffsetBytes / int64(pa.sectorSizeBytes)
	pb.writeOffsetSectors += sectorCount

	if lastSectorOffsetBytes := int(endOffsetBytes % int64(pa.sectorSizeBytes)); lastSectorOffsetBytes == 0 {
		// Allocation ends at a sector boundary. This means that
		// the next object can be stored without coordinating
		// with us on calls to WriteAt().
		pb.sharedSector = nil
	} else {
		// Allocation ends within a sector. Create a new shared
		// sector, only if it's different from the existing one.
		if pb.sharedSector == nil || sectorCount > 0 {
			pb.sharedSector = &sharedSector{
				data: make([]byte, pb.blockAllocator.sectorSizeBytes),
			}
		}
		pb.sharedSector.writeOffsetBytes = lastSectorOffsetBytes
	}
	w.lastSector = pb.sharedSector

	return func(b buffer.Buffer) BlockPutFinalizer {
		// Ingest the data.
		err := b.IntoWriter(w)
		if err == nil {
			err = w.flush()
		}
		pb.Release()

		return func() (int64, error) {
			return writeOffsetBytes, err
		}
	}
}

// blockDeviceBackedBlockReader reads a blob from underlying storage at
// the right offset. When released, it drops the use count on the
// containing block, so that can be freed when unreferenced.
type blockDeviceBackedBlockReader struct {
	io.SectionReader
	block *blockDeviceBackedBlock
}

func (r *blockDeviceBackedBlockReader) Close() error {
	pa := r.block.blockAllocator
	r.block.Release()
	r.block = nil
	pa.blockAllocatorGetsCompleted.Inc()
	return nil
}

// blockDeviceBackedBlockWriter writes a blob to underlying storage at
// the right offset. It could simply have used an io.SectionWriter if
// that had existed.
type blockDeviceBackedBlockWriter struct {
	blockAllocator *blockDeviceBackedBlockAllocator

	// Sector on the block device against which the next WriteAt()
	// operation needs to be performed.
	offsetSectors int64

	// First sector of data that is shared with previous objects.
	firstSector            *sharedSector
	firstSectorOffsetBytes int

	// Sectors in the middle of the object that aren't completed yet.
	partialSector []byte

	// Last sector of data that is shared with successive objects.
	lastSector *sharedSector
}

func (w *blockDeviceBackedBlockWriter) Write(p []byte) (int, error) {
	pa := w.blockAllocator
	pOriginalSizeBytes := len(p)
	if firstSector := w.firstSector; firstSector != nil {
		// We're still writing data into the first sector. These
		// need to be coordinated with the object that came
		// before it.
		firstSector.lock.Lock()
		copiedSizeBytes := copy(firstSector.data[w.firstSectorOffsetBytes:], p)
		p = p[copiedSizeBytes:]
		w.firstSectorOffsetBytes += copiedSizeBytes

		if w.firstSectorOffsetBytes < pa.sectorSizeBytes {
			// First sector is still not completed. Wait for
			// more data to arrive to complete it.
			firstSector.lock.Unlock()
			return pOriginalSizeBytes - len(p), nil
		}

		// First sector completed.
		_, err := pa.blockDevice.WriteAt(firstSector.data, w.offsetSectors*int64(pa.sectorSizeBytes))
		firstSector.lock.Unlock()
		if err != nil {
			return pOriginalSizeBytes - len(p), err
		}
		w.firstSector = nil
		w.offsetSectors++
	}

	if len(w.partialSector) > 0 {
		// We're writing data into a consecutive sector, for
		// which we've received partial data previously.
		copiedSizeBytes := len(p)
		if max := pa.sectorSizeBytes - len(w.partialSector); copiedSizeBytes > max {
			copiedSizeBytes = max
		}
		w.partialSector = append(w.partialSector, p[:copiedSizeBytes]...)
		p = p[copiedSizeBytes:]

		if len(w.partialSector) < pa.sectorSizeBytes {
			// Partial sector is still not completed. Wait
			// for more data to arrive to complete it.
			return pOriginalSizeBytes - len(p), nil
		}

		// Partial sector completed.
		if _, err := pa.blockDevice.WriteAt(w.partialSector, w.offsetSectors*int64(pa.sectorSizeBytes)); err != nil {
			return pOriginalSizeBytes - len(p), err
		}
		w.partialSector = w.partialSector[:0]
		w.offsetSectors++
	}

	if alignedSize := len(p) / pa.sectorSizeBytes * pa.sectorSizeBytes; alignedSize > 0 {
		// Write as many sectors as possible to storage directly,
		// without copying into a partial sector.
		nWritten, err := pa.blockDevice.WriteAt(p[:alignedSize], w.offsetSectors*int64(pa.sectorSizeBytes))
		writtenSectors := nWritten / pa.sectorSizeBytes
		writtenSizeBytes := writtenSectors * pa.sectorSizeBytes
		p = p[writtenSizeBytes:]
		w.offsetSectors += int64(writtenSectors)
		if err != nil {
			return pOriginalSizeBytes - len(p), err
		}
	}

	if len(p) > 0 {
		// Copy trailing data into a new partial sector.
		if w.partialSector == nil {
			w.partialSector = make([]byte, 0, pa.sectorSizeBytes)
		}
		w.partialSector = append(w.partialSector, p...)
	}
	return pOriginalSizeBytes, nil
}

func (w *blockDeviceBackedBlockWriter) flush() error {
	lastSector := w.lastSector
	if lastSector == nil {
		// Write already finished at sector boundary.
		return nil
	}

	lastSector.lock.Lock()
	defer lastSector.lock.Unlock()

	// Combine trailing data with the sector that contains the start
	// of the next object.
	copy(lastSector.data, w.partialSector)
	pa := w.blockAllocator
	_, err := pa.blockDevice.WriteAt(lastSector.data, w.offsetSectors*int64(len(w.lastSector.data)))
	return err
}
