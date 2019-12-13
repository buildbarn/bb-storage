package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type readCachingBlobAccess struct {
	slow BlobAccess
	fast BlobAccess
}

// NewReadCachingBlobAccess turns a fast data store into a read cache
// for a slow data store. All writes are performed against the slow data
// store directly. The slow data store is only accessed for reading in
// case the fast data store does not contain the blob. The blob is then
// streamed into the fast data store.
func NewReadCachingBlobAccess(slow BlobAccess, fast BlobAccess) BlobAccess {
	return &readCachingBlobAccess{
		slow: slow,
		fast: fast,
	}
}

func (ba *readCachingBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	return buffer.WithErrorHandler(
		ba.fast.Get(ctx, digest),
		&readCachingErrorHandler{
			blobAccess: ba,
			context:    ctx,
			digest:     digest,
		})
}

func (ba *readCachingBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	return ba.slow.Put(ctx, digest, b)
}

func (ba *readCachingBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	return ba.slow.FindMissing(ctx, digests)
}

type readCachingErrorHandler struct {
	blobAccess *readCachingBlobAccess
	context    context.Context
	digest     *util.Digest
}

func (eh *readCachingErrorHandler) OnError(observedErr error) (buffer.Buffer, error) {
	if eh.blobAccess == nil || status.Code(observedErr) != codes.NotFound {
		return nil, observedErr
	}
	ba := eh.blobAccess
	eh.blobAccess = nil
	b1, b2 := ba.slow.Get(eh.context, eh.digest).CloneStream()
	b1, t := buffer.WithBackgroundTask(b1)
	go func() { t.Finish(ba.fast.Put(eh.context, eh.digest, b2)) }()
	return b1, nil
}

func (eh *readCachingErrorHandler) Done() {}
