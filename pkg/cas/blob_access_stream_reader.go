package cas

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type blobAccessStreamReader struct {
	blobAccess blobstore.BlobAccess
}

// NewBlobAccessStreamReader creates a StreamReader from a BlobAccess.
func NewBlobAccessStreamReader(contentAddressableStorage blobstore.BlobAccess) StreamReader {
	return &blobAccessStreamReader{
		blobAccess: contentAddressableStorage,
	}
}

func (r *blobAccessStreamReader) ReadStream(ctx context.Context, d digest.Digest) (io.ReadCloser, error) {
	return r.blobAccess.Get(ctx, d).ToReader(), nil
}
