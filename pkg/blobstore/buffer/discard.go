package buffer

import (
	"io"
	"io/ioutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// discardFromReader reads a given amount of data from an io.Reader and
// discards it. This is used for error retrying, where the second
// attempt to fetch data needs to discard the part that was already
// fetched during the first iteration.
func discardFromReader(r io.Reader, off int64) error {
	if off < 0 {
		return status.Errorf(codes.InvalidArgument, "Negative read offset: %d", off)
	}
	_, err := io.CopyN(ioutil.Discard, r, off)
	return err
}

// discardFromChunkReader reads a given amount of data from a
// ChunkReader and discards it. This is used for error retrying, where
// the second attempt to fetch data needs to discard the part that was
// already fetched during the first iteration.
func discardFromChunkReader(r ChunkReader, off int64) ([]byte, error) {
	if off < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Negative read offset: %d", off)
	}
	for off > 0 {
		chunk, err := r.Read()
		if err != nil {
			return nil, err
		}
		if off < int64(len(chunk)) {
			return chunk[off:], nil
		}
		off -= int64(len(chunk))
	}
	return nil, nil
}
