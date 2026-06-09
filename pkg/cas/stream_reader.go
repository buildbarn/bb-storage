package cas

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

// StreamReader reads a blob from the Content Addressable Storage (CAS)
// in a streaming fashion.
type StreamReader interface {
	// Read a blob from the CAS with the specific digest.
	ReadStream(ctx context.Context, d digest.Digest) (io.ReadCloser, error)
}

type streamReader struct {
	contentAddressableStorage ContentAddressableStorage
}

// NewStreamReader creates a stream reader that reads from the provided
// Content Addressable Storage (CAS).
func NewStreamReader(contentAddressableStorage ContentAddressableStorage) StreamReader {
	return &streamReader{
		contentAddressableStorage: contentAddressableStorage,
	}
}

func (r *streamReader) ReadStream(ctx context.Context, d digest.Digest) (io.ReadCloser, error) {
	return GetReadCloser(ctx, r.contentAddressableStorage, d)
}
