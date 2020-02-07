package blobstore

import (
	"context"
	"log"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type errorBlobAccess struct {
	err error
}

// NewErrorBlobAccess creates a BlobAccess that returns a fixed error
// response. Such an implementation is useful for adding explicit
// rejection of oversized requests or disabling storage entirely.
func NewErrorBlobAccess(err error) BlobAccess {
	if err == nil {
		log.Fatal("Attempted to create error blob access with nil error")
	}
	return &errorBlobAccess{
		err: err,
	}
}

func (ba *errorBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return buffer.NewBufferFromError(ba.err)
}

func (ba *errorBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	b.Discard()
	return ba.err
}

func (ba *errorBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, ba.err
}
