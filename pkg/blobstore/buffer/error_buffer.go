package buffer

import (
	"io"

	"google.golang.org/protobuf/proto"
)

type errorBuffer struct {
	err error
}

// NewBufferFromError creates a Buffer that returns a fixed error
// response for all operations.
func NewBufferFromError(err error) Buffer {
	return errorBuffer{
		err: err,
	}
}

func (b errorBuffer) GetSizeBytes() (int64, error) {
	return 0, b.err
}

func (b errorBuffer) IntoWriter(w io.Writer) error {
	return b.err
}

func (b errorBuffer) ReadAt(p []byte, off int64) (int, error) {
	return 0, b.err
}

func (b errorBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return nil, b.err
}

func (b errorBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	return nil, b.err
}

func (b errorBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newErrorChunkReader(b.err)
}

func (b errorBuffer) ToReader() io.ReadCloser {
	return newErrorReader(b.err)
}

func (b errorBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return b, b
}

func (b errorBuffer) CloneStream() (Buffer, Buffer) {
	return b, b
}

func (b errorBuffer) WithTask(task func() error) Buffer {
	// This buffer is trivially cloneable, so we can run the task in
	// the foreground. Discard the error response, because this
	// buffer is already in an error state.
	task()
	return b
}

func (b errorBuffer) Discard() {}

func (b errorBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// The buffer is in a known error state. Invoke the error
	// handler immediately. Either substitute the error message or
	// yield a new buffer.
	newB, transformedErr := errorHandler.OnError(b.err)
	if transformedErr != nil {
		errorHandler.Done()
		return errorBuffer{err: transformedErr}, false
	}
	return newB, true
}

func (b errorBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return newErrorChunkReader(b.err)
}

func (b errorBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return newErrorReader(b.err)
}
