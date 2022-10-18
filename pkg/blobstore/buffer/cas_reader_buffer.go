package buffer

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type casReaderBuffer struct {
	digest digest.Digest
	r      io.ReadCloser
	source Source
}

// NewCASBufferFromReader creates a buffer for an object stored in the
// Content Addressable Storage, whose contents may be obtained through a
// ReadCloser.
func NewCASBufferFromReader(digest digest.Digest, r io.ReadCloser, source Source) Buffer {
	return &casReaderBuffer{
		digest: digest,
		r:      r,
		source: source,
	}
}

func (b *casReaderBuffer) GetSizeBytes() (int64, error) {
	return b.digest.GetSizeBytes(), nil
}

func (b *casReaderBuffer) toValidatedReader() io.ReadCloser {
	return newCASValidatingReader(b.r, b.digest, b.source)
}

func (b *casReaderBuffer) IntoWriter(w io.Writer) error {
	r := b.toValidatedReader()
	defer r.Close()

	_, err := io.Copy(w, r)
	return err
}

func (b *casReaderBuffer) ReadAt(p []byte, off int64) (int, error) {
	r := b.toValidatedReader()
	defer r.Close()

	// Discard the part leading up to the correct offset.
	if err := discardFromReader(r, off); err != nil {
		return 0, err
	}

	// Read the part of data at the correct offset.
	n, err := io.ReadFull(r, p)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return n, io.EOF
	} else if err != nil {
		return 0, err
	}

	// Continue reading the rest of the chunk to force checksum
	// validation.
	if _, err := io.Copy(io.Discard, r); err != nil {
		return 0, err
	}
	return n, nil
}

func (b *casReaderBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return toProtoViaByteSlice(b, m, maximumSizeBytes)
}

func (b *casReaderBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	r := b.toValidatedReader()
	defer r.Close()

	expectedSizeBytes := b.digest.GetSizeBytes()
	if expectedSizeBytes > int64(maximumSizeBytes) {
		return nil, status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a maximum of %d bytes is permitted", expectedSizeBytes, maximumSizeBytes)
	}
	return io.ReadAll(r)
}

func (b *casReaderBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := validateReaderOffset(b.digest.GetSizeBytes(), off); err != nil {
		b.r.Close()
		return newErrorChunkReader(err)
	}

	r := b.toValidatedReader()
	if err := discardFromReader(r, off); err != nil {
		r.Close()
		return newErrorChunkReader(err)
	}
	return newReaderBackedChunkReader(r, maximumChunkSizeBytes)
}

func (b *casReaderBuffer) ToReader() io.ReadCloser {
	return b.toValidatedReader()
}

func (b *casReaderBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return cloneCopyViaByteSlice(b, maximumSizeBytes)
}

func (b *casReaderBuffer) CloneStream() (Buffer, Buffer) {
	return newCASClonedBuffer(b, b.digest, b.source).CloneStream()
}

func (b *casReaderBuffer) WithTask(task func() error) Buffer {
	return newCASBufferWithBackgroundTask(b, b.digest, b.source, task)
}

func (b *casReaderBuffer) Discard() {
	b.r.Close()
}

func (b *casReaderBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// For stream-backed buffers, it is not yet known whether they
	// may be read successfully. Wrap the buffer into one that
	// handles I/O errors upon access.
	return newCASErrorHandlingBuffer(b, errorHandler, b.digest, b.source), false
}

func (b *casReaderBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := discardFromReader(b.r, off); err != nil {
		b.r.Close()
		return newErrorChunkReader(err)
	}
	return newReaderBackedChunkReader(b.r, maximumChunkSizeBytes)
}

func (b *casReaderBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	if err := discardFromReader(b.r, off); err != nil {
		b.r.Close()
		return newErrorReader(err)
	}
	return b.r
}
