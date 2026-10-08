package util

import (
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ReadFullyDecompressed reads exactly size bytes from r into a newly
// allocated buffer. Upon success it additionally verifies that the
// stream contains no more data, meaning that r is consumed up to and
// including its end. This is needed to validate compressed
// representations of blobs, which may expand to more data than the size
// that was originally advertised.
func ReadFullyDecompressed(r io.Reader, size int64) ([]byte, error) {
	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, status.Error(codes.InvalidArgument, "Failed to read blob, data shorter than advertised")
		}
		return nil, err
	}
	var eofBuf [1]byte
	if n, err := r.Read(eofBuf[:]); n > 0 {
		return nil, status.Error(codes.InvalidArgument, "Trailing data after blob")
	} else if err != io.EOF {
		// The stream is malformed or broken in a way that may carry
		// context specific information (e.g. a truncated deflate stream
		// or a deadline that was exceeded). Return the error as-is.
		return nil, err
	}
	return data, nil
}
