package buffer

import (
	"io"

	"google.golang.org/protobuf/proto"
)

// Buffer of data to be read from/written to the Action Cache (AC) or
// Content Addressable Storage (CAS).
//
// BlobAccess implementations gain access to data through a variety of
// APIs. Some of these APIs present data as slices of bytes, while
// others provide io.{Reader,ReadCloser} objects. Similarly, on the
// consumer side, data may also need to be read/written in different
// formats.
//
// Buffers are an abstraction on top of slices and readers, allowing
// data to be converted from one format to the other. It also attempts
// to prevent conversions/copying when unnecessary. For example, when
// creating a buffer from a slice of bytes, a call to ToByteSlice() will
// return the original slice.
//
// Buffers also attempt to ensure the data is consistent. In the case of
// buffers created using NewProtoBufferFrom*(), data may only be
// extracted in case the provided data corresponds to a valid Protobuf
// message. In the case of buffers created using NewCASBufferFrom*(),
// data may only be extracted in case the size and checksum match the
// digest.
type Buffer interface {
	// Return the size of the data stored in the buffer. This
	// function may fail if the buffer is in a known error state in
	// which the size of the object is unknown.
	GetSizeBytes() (int64, error)

	// Of the public functions below, exactly one must be called to
	// release any resources associated with the buffer (e.g., an
	// io.ReadCloser).

	// Write the entire contents of the buffer into a Writer.
	IntoWriter(w io.Writer) error
	// Read a part of the buffer into a byte slice.
	ReadAt(p []byte, off int64) (int, error)
	// Return the contents in the form of an unmarshaled
	// Protobuf message.
	//
	// If and only if the buffer isn't already backed by a Protobuf
	// message, the provided message is used to store the
	// unmarshaled message. The caller must use a type assertion to
	// convert this function's return value back to the appropriate
	// message type.
	ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error)
	// Return the full contents of the buffer as a byte slice.
	ToByteSlice(maximumSizeBytes int) ([]byte, error)
	// Read the contents of the buffer, starting at a given offset,
	// as a stream of byte slices. Normally used by the Content
	// Addressable Storage.
	ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader
	// Obtain a reader that returns the entire contents of the
	// buffer.
	ToReader() io.ReadCloser
	// Obtain two handles to the same underlying object in such a
	// way that they may get copied. This function may be used when
	// buffers need to be inspected prior to returning them.
	CloneCopy(maximumSizeBytes int) (Buffer, Buffer)
	// Obtain two handles to the same underlying object in such a
	// way that the underlying stream may be multiplexed. This
	// function may be used if a single buffer is passed to multiple
	// goroutines that process the same data.
	//
	// It is not safe to use both buffers within same goroutine, as
	// this may cause deadlocks. Contents are only returned when
	// both buffers are accessed.
	CloneStream() (Buffer, Buffer)
	// Release the object without reading its contents.
	Discard()

	// Install an error handler on top of the buffer that is invoked
	// whenever an I/O error occurs.
	//
	// For simpler kinds of buffers (ones backed by byte slices and
	// errors), this function may attempt to run the error handler
	// immediately. This reduces the amount of indirection in the
	// common case. In those cases the boolean return value of this
	// function may be true, indicating that the caller must call
	// applyErrorHandler again.
	applyErrorHandler(errorHandler ErrorHandler) (replacement Buffer, shouldRetry bool)

	// These two functions are similar to ToChunkReader() and
	// ToReader(), except that they bypass checksum validation.
	// These functions are used in case checksum validation is
	// performed at a higher level.
	//
	// In the case of error retrying, parts of multiple buffers are
	// concatenated. Checksum validation needs to happen across
	// those parts, which is why the individual parts may be read
	// with checksum validation disabled.
	toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader
	toUnvalidatedReader(off int64) io.ReadCloser
}
