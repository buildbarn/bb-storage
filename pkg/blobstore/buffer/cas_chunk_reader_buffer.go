package buffer

import (
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type casChunkReaderBuffer struct {
	digest         *util.Digest
	r              ChunkReader
	repairStrategy RepairStrategy
}

// NewCASBufferFromChunkReader creates a buffer for an object stored in
// the Content Addressable Storage, backed by a ChunkReader.
func NewCASBufferFromChunkReader(digest *util.Digest, r ChunkReader, repairStrategy RepairStrategy) Buffer {
	return &casChunkReaderBuffer{
		digest:         digest,
		r:              r,
		repairStrategy: repairStrategy,
	}
}

func (b *casChunkReaderBuffer) GetSizeBytes() (int64, error) {
	return b.digest.GetSizeBytes(), nil
}

func (b *casChunkReaderBuffer) toValidatedChunkReader() ChunkReader {
	return newCASValidatingChunkReader(b.r, b.digest, b.repairStrategy)
}

func (b *casChunkReaderBuffer) IntoWriter(w io.Writer) error {
	return intoWriterViaChunkReader(b.toValidatedChunkReader(), w)
}

func (b *casChunkReaderBuffer) ReadAt(p []byte, off int64) (int, error) {
	return readAtViaChunkReader(b.toValidatedChunkReader(), p, off)
}

func (b *casChunkReaderBuffer) ToActionResult(maximumSizeBytes int) (*remoteexecution.ActionResult, error) {
	return toActionResultViaByteSlice(b, maximumSizeBytes)
}

func (b *casChunkReaderBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	return toByteSliceViaChunkReader(b.toValidatedChunkReader(), b.digest, maximumSizeBytes)
}

func (b *casChunkReaderBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := validateReaderOffset(b.digest.GetSizeBytes(), off); err != nil {
		b.Discard()
		return newErrorChunkReader(err)
	}
	return newNormalizingChunkReader(newOffsetChunkReader(b.toValidatedChunkReader(), off), maximumChunkSizeBytes)
}

func (b *casChunkReaderBuffer) ToReader() io.ReadCloser {
	return newChunkReaderBackedReader(b.toValidatedChunkReader())
}

func (b *casChunkReaderBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return cloneCopyViaByteSlice(b, maximumSizeBytes)
}

func (b *casChunkReaderBuffer) CloneStream() (Buffer, Buffer) {
	return newCASClonedBuffer(b, b.digest, b.repairStrategy).CloneStream()
}

func (b *casChunkReaderBuffer) Discard() {
	b.r.Close()
}

func (b *casChunkReaderBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// For stream-backed buffers, it is not yet known whether they
	// may be read successfully. Wrap the buffer into one that
	// handles I/O errors upon access.
	return newCASErrorHandlingBuffer(b, errorHandler, b.digest, b.repairStrategy), false
}

func (b *casChunkReaderBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newNormalizingChunkReader(newOffsetChunkReader(b.r, off), maximumChunkSizeBytes)
}

func (b *casChunkReaderBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return newChunkReaderBackedReader(newOffsetChunkReader(b.r, off))
}
