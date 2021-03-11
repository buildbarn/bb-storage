package buffer

import (
	"io"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

const (
	defaultChunkSizeBytes = 64 * 1024
)

type casClonedBuffer struct {
	base   Buffer
	digest digest.Digest
	source Source

	lock                  sync.Mutex
	consumersRemaining    uint
	consumersWaiting      []chan ChunkReader
	needsValidation       bool
	maximumChunkSizeBytes int
}

// newCASClonedBuffer creates a decorator for CAS-backed buffer objects
// that permits concurrent access to the same buffer. All consumers will
// be synchronized, meaning that they will get access to the buffer's
// contents at the same pace.
func newCASClonedBuffer(base Buffer, digest digest.Digest, source Source) Buffer {
	return &casClonedBuffer{
		base:   base,
		digest: digest,
		source: source,

		consumersRemaining:    1,
		maximumChunkSizeBytes: -1,
	}
}

func (b *casClonedBuffer) GetSizeBytes() (int64, error) {
	return b.digest.GetSizeBytes(), nil
}

func (b *casClonedBuffer) toChunkReader(needsValidation bool, maximumChunkSizeBytes int) ChunkReader {
	b.lock.Lock()
	if b.consumersRemaining == 0 {
		panic("Attempted to obtain a chunk reader for a buffer that is already fully consumed")
	}
	b.consumersRemaining--

	// Provide constraints that this consumer desires.
	b.needsValidation = b.needsValidation || needsValidation
	if b.maximumChunkSizeBytes < 0 || b.maximumChunkSizeBytes > maximumChunkSizeBytes {
		b.maximumChunkSizeBytes = maximumChunkSizeBytes
	}

	// Create the underlying ChunkReader in case all consumers have
	// supplied their constraints.
	if b.consumersRemaining == 0 {
		// If there is at least one consumer that needs checksum
		// validation, we use checksum validation for everyone.
		var r ChunkReader
		if b.needsValidation {
			r = b.base.ToChunkReader(0, b.maximumChunkSizeBytes)
		} else {
			r = b.base.toUnvalidatedChunkReader(0, b.maximumChunkSizeBytes)
		}

		// Give all consumers their own ChunkReader.
		rMultiplexed := newMultiplexedChunkReader(r, len(b.consumersWaiting))
		for _, c := range b.consumersWaiting {
			c <- rMultiplexed
		}
		b.lock.Unlock()
		return rMultiplexed
	}

	// There are other consumers that still have to supply their
	// constraints. Let the last consumer create the ChunkReader and
	// hand it out.
	c := make(chan ChunkReader, 1)
	b.consumersWaiting = append(b.consumersWaiting, c)
	b.lock.Unlock()
	return <-c
}

func (b *casClonedBuffer) IntoWriter(w io.Writer) error {
	return intoWriterViaChunkReader(b.toChunkReader(true, defaultChunkSizeBytes), w)
}

func (b *casClonedBuffer) ReadAt(p []byte, off int64) (int, error) {
	return readAtViaChunkReader(b.toChunkReader(true, defaultChunkSizeBytes), p, off)
}

func (b *casClonedBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return toProtoViaByteSlice(b, m, maximumSizeBytes)
}

func (b *casClonedBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	return toByteSliceViaChunkReader(b.toChunkReader(true, defaultChunkSizeBytes), b.digest, maximumSizeBytes)
}

func (b *casClonedBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newOffsetChunkReader(b.toChunkReader(true, maximumChunkSizeBytes), off)
}

func (b *casClonedBuffer) ToReader() io.ReadCloser {
	return newChunkReaderBackedReader(b.toChunkReader(true, defaultChunkSizeBytes))
}

func (b *casClonedBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return cloneCopyViaByteSlice(b, maximumSizeBytes)
}

func (b *casClonedBuffer) CloneStream() (Buffer, Buffer) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.consumersRemaining == 0 {
		panic("Attempted to clone stream for a buffer that is already fully consumed")
	}
	b.consumersRemaining++
	return b, b
}

func (b *casClonedBuffer) Discard() {
	b.toChunkReader(false, defaultChunkSizeBytes).Close()
}

func (b *casClonedBuffer) applyErrorHandler(errorHandler ErrorHandler) (replacement Buffer, shouldRetry bool) {
	// For stream-backed buffers, it is not yet known whether they
	// may be read successfully. Wrap the buffer into one that
	// handles I/O errors upon access.
	return newCASErrorHandlingBuffer(b, errorHandler, b.digest, b.source), false
}

func (b *casClonedBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newOffsetChunkReader(b.toChunkReader(false, maximumChunkSizeBytes), off)
}

func (b *casClonedBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return newChunkReaderBackedReader(b.toUnvalidatedChunkReader(off, defaultChunkSizeBytes))
}
