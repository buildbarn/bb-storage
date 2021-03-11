package buffer

import (
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

type casErrorHandlingBuffer struct {
	base         Buffer
	errorHandler ErrorHandler
	digest       digest.Digest
	source       Source
}

// newCASErrorHandlingBuffer is a decorator for Buffer that handles I/O
// errors by passing them to an ErrorHandler. The ErrorHandler is
// capable of returning an alternative buffer that should be used to
// continue the transfer. This decorator will retry/resume the same call
// against the new buffer.
func newCASErrorHandlingBuffer(base Buffer, errorHandler ErrorHandler, digest digest.Digest, source Source) Buffer {
	return &casErrorHandlingBuffer{
		base:         base,
		errorHandler: errorHandler,
		digest:       digest,
		source:       source,
	}
}

func (b *casErrorHandlingBuffer) GetSizeBytes() (int64, error) {
	return b.digest.GetSizeBytes(), nil
}

// tryRepeatedly implements the retrying strategy for buffer operations
// that can safely be retried in their entirety, without causing partial
// data to be written twice.
func (b *casErrorHandlingBuffer) tryRepeatedly(f func(Buffer) error) error {
	defer b.errorHandler.Done()
	base := b.base
	for {
		// Attempt to apply the operation against the buffer.
		originalErr := f(base)
		if originalErr == nil || originalErr == io.EOF {
			return originalErr
		}

		// Operation failed. Call into the error handler to
		// either adjust the error or return a new buffer
		// against which to retry the operation.
		var translatedErr error
		base, translatedErr = b.errorHandler.OnError(originalErr)
		if translatedErr != nil {
			return translatedErr
		}
	}
}

func (b *casErrorHandlingBuffer) toValidatedChunkReader(maximumChunkSizeBytes int) ChunkReader {
	return newCASValidatingChunkReader(b.toUnvalidatedChunkReader(0, maximumChunkSizeBytes), b.digest, b.source)
}

func (b *casErrorHandlingBuffer) IntoWriter(w io.Writer) error {
	// This operation cannot use tryRepeatedly(), as individual
	// retries may write parts to the output stream. Copy into the
	// output stream using a retrying ChunkReader.
	return intoWriterViaChunkReader(b.toValidatedChunkReader(64*1024), w)
}

func (b *casErrorHandlingBuffer) ReadAt(p []byte, off int64) (n int, translatedErr error) {
	translatedErr = b.tryRepeatedly(func(base Buffer) error {
		var originalErr error
		n, originalErr = base.ReadAt(p, off)
		return originalErr
	})
	return
}

func (b *casErrorHandlingBuffer) ToProto(m proto.Message, maximumSizeBytes int) (mResult proto.Message, translatedErr error) {
	translatedErr = b.tryRepeatedly(func(base Buffer) error {
		var originalErr error
		mResult, originalErr = base.ToProto(m, maximumSizeBytes)
		return originalErr
	})
	return
}

func (b *casErrorHandlingBuffer) ToByteSlice(maximumSizeBytes int) (data []byte, translatedErr error) {
	translatedErr = b.tryRepeatedly(func(base Buffer) error {
		var originalErr error
		data, originalErr = base.ToByteSlice(maximumSizeBytes)
		return originalErr
	})
	return
}

func (b *casErrorHandlingBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := validateReaderOffset(b.digest.GetSizeBytes(), off); err != nil {
		b.Discard()
		return newErrorChunkReader(err)
	}
	return newOffsetChunkReader(b.toValidatedChunkReader(maximumChunkSizeBytes), off)
}

func (b *casErrorHandlingBuffer) ToReader() io.ReadCloser {
	return newCASValidatingReader(b.toUnvalidatedReader(0), b.digest, b.source)
}

func (b *casErrorHandlingBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return cloneCopyViaByteSlice(b, maximumSizeBytes)
}

func (b *casErrorHandlingBuffer) CloneStream() (Buffer, Buffer) {
	return newCASClonedBuffer(b, b.digest, b.source).CloneStream()
}

func (b *casErrorHandlingBuffer) Discard() {
	b.errorHandler.Done()
	b.base.Discard()
}

func (b *casErrorHandlingBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// For stream-backed buffers, it is not yet known whether they
	// may be read successfully. Wrap the buffer into one that
	// handles I/O errors upon access.
	return newCASErrorHandlingBuffer(b, errorHandler, b.digest, b.source), false
}

func (b *casErrorHandlingBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newErrorHandlingChunkReader(b.base, b.errorHandler, off, maximumChunkSizeBytes)
}

func (b *casErrorHandlingBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return newErrorHandlingReader(b.base, b.errorHandler, off)
}
