package buffer

import (
	"io"
	"io/ioutil"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// acBuffer stores a copy of valid objects stored in the Action Cache.
// The structure holds both a marshaled and unmarshaled copy of an
// ActionResult, so that any access is guaranteed to succeed.
type acBuffer struct {
	validatedByteSliceBuffer
	actionResult remoteexecution.ActionResult
}

// NewACBufferFromActionResult creates a buffer for an object stored in
// the Action Cache, based on an unmarshaled ActionResult message.
func NewACBufferFromActionResult(actionResult *remoteexecution.ActionResult, repairStrategy RepairStrategy) Buffer {
	data, err := proto.Marshal(actionResult)
	if err != nil {
		return NewBufferFromError(repairStrategy.repairACMarshalFailure(err))
	}
	return &acBuffer{
		validatedByteSliceBuffer: validatedByteSliceBuffer{data: data},
		actionResult:             *actionResult,
	}
}

// NewACBufferFromByteSlice creates a buffer for an object stored in the
// Action Cache, based on a marshaled ActionResult message stored in a
// byte slice.
func NewACBufferFromByteSlice(data []byte, repairStrategy RepairStrategy) Buffer {
	b := &acBuffer{
		validatedByteSliceBuffer: validatedByteSliceBuffer{data: data},
	}
	if err := proto.Unmarshal(data, &b.actionResult); err != nil {
		return NewBufferFromError(repairStrategy.repairACUnmarshalFailure(err))
	}
	return b
}

// NewACBufferFromReader creates a buffer for an object stored in the
// Action Cache, based on a marshaled ActionResult message that may be
// obtained through a ReadCloser.
func NewACBufferFromReader(r io.ReadCloser, repairStrategy RepairStrategy) Buffer {
	// Messages in the Action Cache are relatively small, so it's
	// safe to keep them in memory. Read and store them in a byte
	// slice buffer immediately. This permits implementing
	// GetSizeBytes() properly.
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return NewBufferFromError(err)
	}
	return NewACBufferFromByteSlice(data, repairStrategy)
}

func (b *acBuffer) ToActionResult(maximumSizeBytes int) (*remoteexecution.ActionResult, error) {
	if len(b.validatedByteSliceBuffer.data) > maximumSizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a maximum of %d bytes is permitted", len(b.data), maximumSizeBytes)
	}
	return &b.actionResult, nil
}

func (b *acBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return b, b
}

func (b *acBuffer) CloneStream() (Buffer, Buffer) {
	return b, b
}

func (b *acBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	// The buffer is in a known good state. Terminate the error
	// handler directly. There is no need to return a wrapped buffer.
	errorHandler.Done()
	return b, false
}
