package buffer

import (
	"bytes"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type validatedByteSliceBuffer struct {
	data []byte
}

// NewValidatedBufferFromByteSlice creates a Buffer that is backed by a
// slice of bytes. No checking of data integrity is performed, as it is
// assumed that the data stored in the slice is valid.
func NewValidatedBufferFromByteSlice(data []byte) Buffer {
	return &validatedByteSliceBuffer{
		data: data,
	}
}

// NewCASBufferFromByteSlice creates a buffer for an object stored in
// the Content Addressable Storage, backed by a byte slice.
func NewCASBufferFromByteSlice(digest digest.Digest, data []byte, source Source) Buffer {
	// Compare the blob's size.
	expectedSizeBytes := digest.GetSizeBytes()
	actualSizeBytes := int64(len(data))
	if expectedSizeBytes != actualSizeBytes {
		return NewBufferFromError(source.notifyCASSizeMismatch(expectedSizeBytes, actualSizeBytes))
	}

	// Compare the blob's checksum.
	expectedChecksum := digest.GetHashBytes()
	hasher := digest.NewHasher(actualSizeBytes)
	hasher.Write(data)
	actualChecksum := hasher.Sum(nil)
	if bytes.Compare(expectedChecksum, actualChecksum) != 0 {
		return NewBufferFromError(source.notifyCASHashMismatch(expectedChecksum, actualChecksum))
	}

	source.notifyDataValid()
	return NewValidatedBufferFromByteSlice(data)
}

func (b validatedByteSliceBuffer) GetSizeBytes() (int64, error) {
	return int64(len(b.data)), nil
}

func (b validatedByteSliceBuffer) IntoWriter(w io.Writer) error {
	_, err := w.Write(b.data)
	return err
}

func (b validatedByteSliceBuffer) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, status.Errorf(codes.InvalidArgument, "Negative read offset: %d", off)
	}
	if off > int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (b validatedByteSliceBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return toProtoViaByteSlice(b, m, maximumSizeBytes)
}

func (b validatedByteSliceBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	if len(b.data) > maximumSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a maximum of %d bytes is permitted", len(b.data), maximumSizeBytes)
	}
	return b.data, nil
}

func (b validatedByteSliceBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.toUnvalidatedChunkReader(off, maximumChunkSizeBytes)
}

func (b validatedByteSliceBuffer) ToReader() io.ReadCloser {
	return b.toUnvalidatedReader(0)
}

func (b validatedByteSliceBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return b, b
}

func (b validatedByteSliceBuffer) CloneStream() (Buffer, Buffer) {
	return b, b
}

func (b validatedByteSliceBuffer) WithTask(task func() error) Buffer {
	// This buffer is trivially cloneable, so we can run the task in
	// the foreground.
	if err := task(); err != nil {
		return NewBufferFromError(err)
	}
	return b
}

func (validatedByteSliceBuffer) Discard() {}

func (b validatedByteSliceBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// The buffer is in a known good state. Terminate the error
	// handler directly. There is no need to return a wrapped buffer.
	errorHandler.Done()
	return b, false
}

func (b validatedByteSliceBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := validateReaderOffset(int64(len(b.data)), off); err != nil {
		return newErrorChunkReader(err)
	}
	return &byteSliceChunkReader{
		maximumChunkSizeBytes: maximumChunkSizeBytes,
		data:                  b.data[off:],
	}
}

func (b validatedByteSliceBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	if err := validateReaderOffset(int64(len(b.data)), off); err != nil {
		return newErrorReader(err)
	}
	return io.NopCloser(bytes.NewBuffer(b.data[off:]))
}

type byteSliceChunkReader struct {
	maximumChunkSizeBytes int
	data                  []byte
}

func (r *byteSliceChunkReader) Read() ([]byte, error) {
	data := r.data
	if len(data) == 0 {
		// No more data to return.
		return nil, io.EOF
	}
	if len(data) <= r.maximumChunkSizeBytes {
		// Last chunk of data to be returned.
		r.data = nil
		return data, nil
	}
	// Full chunk of data still available.
	r.data = r.data[r.maximumChunkSizeBytes:]
	return data[:r.maximumChunkSizeBytes], nil
}

func (byteSliceChunkReader) Close() {}
