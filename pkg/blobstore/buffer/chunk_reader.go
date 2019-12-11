package buffer

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ChunkReader is similar to io.ReadCloser, except that data is not
// copied from the stream into an output array. The implementation is
// responsible for providing space for the data. This interface is
// similar to how frame-based transfer protocols work, including the
// Bytestream protocol used by REv2.
type ChunkReader interface {
	Read() ([]byte, error)
	Close()
}

// validateReaderOffset is used by ToChunkReader() to validate the
// offset that is provided. The interface does not permit reading at
// negative offsets or beyond the end of the object.
func validateReaderOffset(length int64, requested int64) error {
	if requested < 0 {
		return status.Errorf(codes.InvalidArgument, "Negative read offset: %d", requested)
	}
	if requested > length {
		return status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a read at offset %d was requested", length, requested)
	}
	return nil
}
