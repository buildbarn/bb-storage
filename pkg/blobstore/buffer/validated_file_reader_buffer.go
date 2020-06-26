package buffer

import (
	"io"
	"io/ioutil"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type validatedReaderBuffer struct {
	r          filesystem.FileReader
	sizeBytes  int64
	cloneCount int32
}

// NewValidatedBufferFromFileReader creates a Buffer that is backed by a
// FileReader. No checking of data integrity is performed, as it is
// assumed that the data stored in the slice is valid.
//
// This function should be used with care, as media backing FileReaders
// (e.g., local file systems, block devices) may well be prone to data
// corruption. This will not be detected if buffers are constructed
// using this function.
//
// The provided FileReader must permit ReadAt() to be called in
// parallel, as cloning the buffer may permit multiple goroutines to
// access the data.
func NewValidatedBufferFromFileReader(r filesystem.FileReader, sizeBytes int64) Buffer {
	return &validatedReaderBuffer{
		r:         r,
		sizeBytes: sizeBytes,
	}
}

func (b *validatedReaderBuffer) GetSizeBytes() (int64, error) {
	return b.sizeBytes, nil
}

func (b *validatedReaderBuffer) IntoWriter(w io.Writer) error {
	defer b.Discard()
	_, err := io.Copy(w, io.NewSectionReader(b.r, 0, b.sizeBytes))
	return err
}

func (b *validatedReaderBuffer) ReadAt(p []byte, off int64) (int, error) {
	defer b.Discard()
	return b.r.ReadAt(p, off)
}

func (b *validatedReaderBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return toProtoViaByteSlice(b, m, maximumSizeBytes)
}

func (b *validatedReaderBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	defer b.Discard()

	if b.sizeBytes > int64(maximumSizeBytes) {
		return nil, status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a maximum of %d bytes is permitted", b.sizeBytes, maximumSizeBytes)
	}
	return ioutil.ReadAll(io.NewSectionReader(b.r, 0, b.sizeBytes))
}

func (b *validatedReaderBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.toUnvalidatedChunkReader(off, maximumChunkSizeBytes)
}

func (b *validatedReaderBuffer) ToReader() io.ReadCloser {
	return &validatedFileReaderReader{
		SectionReader: *io.NewSectionReader(b.r, 0, b.sizeBytes),
		b:             b,
	}
}

func (b *validatedReaderBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	atomic.AddInt32(&b.cloneCount, 1)
	return b, b
}

func (b *validatedReaderBuffer) CloneStream() (Buffer, Buffer) {
	atomic.AddInt32(&b.cloneCount, 1)
	return b, b
}

func (b *validatedReaderBuffer) Discard() {
	if atomic.AddInt32(&b.cloneCount, -1) < 0 {
		// There are no more cloned instances of this buffer.
		b.r.Close()
		b.r = nil
	}
}

func (b *validatedReaderBuffer) applyErrorHandler(errorHandler ErrorHandler) (replacement Buffer, shouldRetry bool) {
	// TODO: Add support for actually respecting the error handler.
	// This is currently hard to achieve, as cloning the buffer may
	// cause the underlying reader to be accessed concurrently.
	// Error handlers may currently only be invoked sequentially.
	//
	// Right now this is not causing any loss of functionality, as
	// the FileReaders currently provided to
	// NewValidatedBufferFromFileReader cannot realistically fail.
	errorHandler.Done()
	return b, false
}

func (b *validatedReaderBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := validateReaderOffset(b.sizeBytes, off); err != nil {
		b.Discard()
		return newErrorChunkReader(err)
	}
	return newReaderBackedChunkReader(b.toUnvalidatedReader(off), maximumChunkSizeBytes)
}

func (b *validatedReaderBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	if err := validateReaderOffset(b.sizeBytes, off); err != nil {
		b.Discard()
		return newErrorReader(err)
	}
	return &validatedFileReaderReader{
		SectionReader: *io.NewSectionReader(b.r, off, b.sizeBytes-off),
		b:             b,
	}
}

type validatedFileReaderReader struct {
	io.SectionReader
	b *validatedReaderBuffer
}

func (r *validatedFileReaderReader) Close() error {
	r.b.Discard()
	return nil
}
