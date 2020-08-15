package buffer

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/protobuf/proto"
)

type casChunkReaderBuffer struct {
	digest digest.Digest
	r      ChunkReader
	source Source
}

// NewCASBufferFromChunkReader creates a buffer for an object stored in
// the Content Addressable Storage, backed by a ChunkReader.
func NewCASBufferFromChunkReader(digest digest.Digest, r ChunkReader, source Source) Buffer {
	return &casChunkReaderBuffer{
		digest: digest,
		r:      r,
		source: source,
	}
}

func (b *casChunkReaderBuffer) GetSizeBytes() (int64, error) {
	return b.digest.GetSizeBytes(), nil
}

func (b *casChunkReaderBuffer) toValidatedChunkReader() ChunkReader {
	return newCASValidatingChunkReader(b.r, b.digest, b.source)
}

func (b *casChunkReaderBuffer) IntoWriter(w io.Writer) error {
	return intoWriterViaChunkReader(b.toValidatedChunkReader(), w)
}

func (b *casChunkReaderBuffer) ReadAt(p []byte, off int64) (int, error) {
	return readAtViaChunkReader(b.toValidatedChunkReader(), p, off)
}

func (b *casChunkReaderBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return toProtoViaByteSlice(b, m, maximumSizeBytes)
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
	return newCASClonedBuffer(b, b.digest, b.source).CloneStream()
}

func (b *casChunkReaderBuffer) Discard() {
	b.r.Close()
}

func (b *casChunkReaderBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// For stream-backed buffers, it is not yet known whether they
	// may be read successfully. Wrap the buffer into one that
	// handles I/O errors upon access.
	return newCASErrorHandlingBuffer(b, errorHandler, b.digest, b.source), false
}

func (b *casChunkReaderBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newNormalizingChunkReader(newOffsetChunkReader(b.r, off), maximumChunkSizeBytes)
}

func (b *casChunkReaderBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return newChunkReaderBackedReader(newOffsetChunkReader(b.r, off))
}
