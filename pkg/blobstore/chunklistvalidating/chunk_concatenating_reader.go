package chunklistvalidating

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// chunkConcatenatingReader is a helper utility that implements the
// io.ReadCloser api over a series of digest.Digest objectes fetched
// sequentially from the CAS.
type chunkConcatenatingReader struct {
	ctx                       context.Context
	contentAddressableStorage blobstore.BlobAccess
	chunkDigests              []digest.Digest
	currentIndex              int
	currentReader             io.ReadCloser
	closed                    bool
}

func (r *chunkConcatenatingReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, status.Error(codes.Internal, "Reader is already closed")
	}
	for {
		if r.currentReader == nil {
			if r.currentIndex >= len(r.chunkDigests) {
				return 0, io.EOF
			}
			chunkDigest := r.chunkDigests[r.currentIndex]
			b := r.contentAddressableStorage.Get(r.ctx, chunkDigest)
			r.currentReader = b.ToReader()
			r.currentIndex++
		}

		n, err := r.currentReader.Read(p)
		if n > 0 {
			return n, nil
		}
		if err == io.EOF {
			err = r.currentReader.Close()
			r.currentReader = nil
			if err != nil {
				return 0, err
			}
			continue
		}
		if err != nil {
			_ = r.currentReader.Close()
			r.currentReader = nil
			return 0, util.StatusWrap(err, "Failed to read chunk")
		}
	}
}

func (r *chunkConcatenatingReader) Close() (err error) {
	r.closed = true
	if r.currentReader != nil {
		err = r.currentReader.Close()
		r.currentReader = nil
	}
	return err
}
