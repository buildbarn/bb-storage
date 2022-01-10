package buffer

import (
	"io"
	"io/ioutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// protoBuffer stores a copy of valid objects contained in data stores
// that use Protobufs, such as the Action Cache. The structure holds
// both a marshaled and unmarshaled copy of a Protobuf message, so that
// any access is guaranteed to succeed.
type protoBuffer struct {
	validatedByteSliceBuffer
	message proto.Message
}

// NewProtoBufferFromProto creates a buffer for an object contained in a
// Protobuf storage such as the Action Cache, based on an unmarshaled
// Protobuf message.
func NewProtoBufferFromProto(message proto.Message, source Source) Buffer {
	data, err := proto.Marshal(message)
	if err != nil {
		return NewBufferFromError(source.notifyProtoMarshalFailure(err))
	}
	source.notifyDataValid()
	return &protoBuffer{
		validatedByteSliceBuffer: validatedByteSliceBuffer{data: data},
		message:                  message,
	}
}

// NewProtoBufferFromByteSlice creates a buffer for an object contained
// in a Protobuf storage such as Action Cache, based on a marshaled
// Protobuf message stored in a byte slice. An empty Protobuf message
// object must be provided that corresponds with the type of the data
// contained in the byte slice.
func NewProtoBufferFromByteSlice(m proto.Message, data []byte, source Source) Buffer {
	if err := proto.Unmarshal(data, m); err != nil {
		return NewBufferFromError(source.notifyProtoUnmarshalFailure(err))
	}
	source.notifyDataValid()
	return &protoBuffer{
		validatedByteSliceBuffer: validatedByteSliceBuffer{data: data},
		message:                  m,
	}
}

// NewProtoBufferFromReader creates a buffer for an object contained in
// a Protobuf storage such as the Action Cache, based on a marshaled
// Protobuf message that may be obtained through a ReadCloser. An empty
// Protobuf message object must be provided that corresponds with the
// type of the data contained in the reader.
func NewProtoBufferFromReader(m proto.Message, r io.ReadCloser, source Source) Buffer {
	// Messages in the Action Cache are relatively small, so it's
	// safe to keep them in memory. Read and store them in a byte
	// slice buffer immediately. This permits implementing
	// GetSizeBytes() properly.
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return NewBufferFromError(err)
	}
	return NewProtoBufferFromByteSlice(m, data, source)
}

func (b *protoBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	if len(b.validatedByteSliceBuffer.data) > maximumSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a maximum of %d bytes is permitted", len(b.data), maximumSizeBytes)
	}
	return b.message, nil
}

func (b *protoBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return b, b
}

func (b *protoBuffer) CloneStream() (Buffer, Buffer) {
	return b, b
}

func (b *protoBuffer) WithTask(task func() error) Buffer {
	// This buffer is trivially cloneable, so we can run the task in
	// the foreground.
	if err := task(); err != nil {
		return NewBufferFromError(err)
	}
	return b
}

func (b *protoBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// The buffer is in a known good state. Terminate the error
	// handler directly. There is no need to return a wrapped buffer.
	errorHandler.Done()
	return b, false
}
