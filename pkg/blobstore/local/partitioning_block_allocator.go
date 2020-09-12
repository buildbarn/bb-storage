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
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	partitioningBlockAllocatorPrometheusMetrics sync.Once

	partitioningBlockAllocatorAllocations = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "partitioning_block_allocator_allocations_total",
			Help:      "Number of times blocks managed by PartitioningBlockAllocator were allocated",
		})
	partitioningBlockAllocatorReleases = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "partitioning_block_allocator_releases_total",
			Help:      "Number of times blocks managed by PartitioningBlockAllocator were released",
		})

	partitioningBlockAllocatorGetsStarted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "partitioning_block_allocator_gets_started_total",
			Help:      "Number of Get() operations PartitioningBlockAllocator that were started",
		})
	partitioningBlockAllocatorGetsCompleted = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "partitioning_block_allocator_gets_completed_total",
			Help:      "Number of Get() operations PartitioningBlockAllocator that were completed",
		})
)

type partitioningBlockAllocator struct {
	blockDevice       blockdevice.BlockDevice
	readBufferFactory blobstore.ReadBufferFactory
	sectorSizeBytes   int

	lock        sync.Mutex
	freeOffsets []int64
}

// NewPartitioningBlockAllocator implements a BlockAllocator that can be
// used by LocalBlobAccess to store data. Blocks created by this
// allocator are backed by a single BlockDevice. Storage is partitioned
// into equally sized blocks that are stored consecutively.
//
// Blocks are initially allocated out by increasing offset. Later on,
// the least recently released blocks are reused. This adds wear
// leveling to the system.
//
// This implementation also ensures that writes against underlying
// storage are all performed at sector boundaries and sizes. This
// ensures that no unnecessary reads are performed.
func NewPartitioningBlockAllocator(blockDevice blockdevice.BlockDevice, readBufferFactory blobstore.ReadBufferFactory, sectorSizeBytes int, blockSectorCount int64, blockCount int) BlockAllocator {
	partitioningBlockAllocatorPrometheusMetrics.Do(func() {
		prometheus.MustRegister(partitioningBlockAllocatorAllocations)
		prometheus.MustRegister(partitioningBlockAllocatorReleases)

		prometheus.MustRegister(partitioningBlockAllocatorGetsStarted)
		prometheus.MustRegister(partitioningBlockAllocatorGetsCompleted)
	})

	pa := &partitioningBlockAllocator{
		blockDevice:       blockDevice,
		readBufferFactory: readBufferFactory,
		sectorSizeBytes:   sectorSizeBytes,
	}
	for i := 0; i < blockCount; i++ {
		pa.freeOffsets = append(pa.freeOffsets, int64(i)*blockSectorCount)
	}
	return pa
}

func (pa *partitioningBlockAllocator) NewBlock() (Block, error) {
	pa.lock.Lock()
	defer pa.lock.Unlock()

	if len(pa.freeOffsets) == 0 {
		return nil, status.Error(codes.ResourceExhausted, "No unused blocks available")
	}
	block := &partitioningBlock{
		blockAllocator: pa,
		offset:         pa.freeOffsets[0],
		usecount:       1,
	}
	pa.freeOffsets = pa.freeOffsets[1:]
	partitioningBlockAllocatorAllocations.Inc()
	return block, nil
}

type partitioningBlock struct {
	blockAllocator *partitioningBlockAllocator
	offset         int64
	usecount       int64
}

func (pb *partitioningBlock) Release() {
	if c := atomic.AddInt64(&pb.usecount, -1); c < 0 {
		panic(fmt.Sprintf("Release(): Block has invalid reference count %d", c))
	} else if c == 0 {
		// Block has no remaining consumers. Allow the region in
		// storage to be reused for new data.
		pa := pb.blockAllocator
		pa.lock.Lock()
		pa.freeOffsets = append(pa.freeOffsets, pb.offset)
		pa.lock.Unlock()
		partitioningBlockAllocatorReleases.Inc()
	}
}

func (pb *partitioningBlock) Get(digest digest.Digest, offsetBytes int64, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
	if c := atomic.AddInt64(&pb.usecount, 1); c <= 1 {
		panic(fmt.Sprintf("Get(): Block has invalid reference count %d", c))
	}
	partitioningBlockAllocatorGetsStarted.Inc()

	return pb.blockAllocator.readBufferFactory.NewBufferFromFileReader(
		digest,
		&partitioningBlockReader{
			SectionReader: *io.NewSectionReader(
				pb.blockAllocator.blockDevice,
				pb.offset*int64(pb.blockAllocator.sectorSizeBytes)+offsetBytes,
				sizeBytes),
			block: pb,
		},
		sizeBytes,
		dataIntegrityCallback)
}

func (pb *partitioningBlock) Put(offsetBytes int64, b buffer.Buffer) error {
	if pb.usecount <= 0 {
		panic("Attempted to store buffer in unused block")
	}

	sectorSizeBytes := pb.blockAllocator.sectorSizeBytes
	if offsetBytes%int64(sectorSizeBytes) != 0 {
		panic("Attempted to store buffer at unaligned location")
	}

	w := &partitioningBlockWriter{
		w:             pb.blockAllocator.blockDevice,
		partialSector: make([]byte, 0, pb.blockAllocator.sectorSizeBytes),
		offset:        pb.offset + offsetBytes/int64(sectorSizeBytes),
	}

	if err := b.IntoWriter(w); err != nil {
		return err
	}
	return w.flush()
}

// partitioningBlockReader reads a blob from underlying storage at the
// right offset. When released, it drops the use count on the containing
// block, so that can be freed when unreferenced.
type partitioningBlockReader struct {
	io.SectionReader
	block *partitioningBlock
}

func (r *partitioningBlockReader) Close() error {
	r.block.Release()
	r.block = nil
	partitioningBlockAllocatorGetsCompleted.Inc()
	return nil
}

// partitioningBlockWriter writes a blob to underlying storage at the
// right offset. It could simply have used an io.SectionWriter if that
// had existed.
type partitioningBlockWriter struct {
	w             io.WriterAt
	partialSector []byte
	offset        int64
}

func (w *partitioningBlockWriter) Write(p []byte) (int, error) {
	sectorSizeBytes := cap(w.partialSector)

	leadingSize := 0
	if len(w.partialSector) > 0 {
		// Copy the leading part of the data into the partial
		// sector that was created previously.
		leadingSize = len(p)
		if remaining := sectorSizeBytes - len(w.partialSector); leadingSize > remaining {
			leadingSize = remaining
		}
		w.partialSector = append(w.partialSector, p[:leadingSize]...)
		if len(w.partialSector) < sectorSizeBytes {
			return leadingSize, nil
		}

		// The partial sector has become full. Write it out to
		// storage.
		if _, err := w.w.WriteAt(w.partialSector, w.offset*int64(sectorSizeBytes)); err != nil {
			return leadingSize, err
		}
		w.partialSector = w.partialSector[:0]
		w.offset++
	}

	// Write as many sectors as possible to storage directly,
	// without copying into a partial sector.
	alignedSize := (len(p) - leadingSize) / sectorSizeBytes * sectorSizeBytes
	n, err := w.w.WriteAt(p[leadingSize:leadingSize+alignedSize], w.offset*int64(sectorSizeBytes))
	writtenSectors := n / sectorSizeBytes
	w.offset += int64(writtenSectors)
	if err != nil {
		return leadingSize + writtenSectors*sectorSizeBytes, err
	}

	// Copy trailing data into a new partial sector.
	w.partialSector = append(w.partialSector, p[leadingSize+alignedSize:]...)
	return len(p), nil
}

func (w *partitioningBlockWriter) flush() error {
	if len(w.partialSector) == 0 {
		return nil
	}

	// Add zero padding to the final sector and write it to storage.
	// Adding the padding ensures that no attempt is made to load
	// the original sector from storage.
	sectorSizeBytes := cap(w.partialSector)
	w.partialSector = append(w.partialSector, make([]byte, sectorSizeBytes-len(w.partialSector))...)
	_, err := w.w.WriteAt(w.partialSector, w.offset*int64(sectorSizeBytes))
	return err
}
