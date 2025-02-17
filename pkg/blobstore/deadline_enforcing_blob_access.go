package blobstore

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type deadlineEnforcingBlobAccess struct {
	delegate BlobAccess
	timeout  time.Duration
}

// NewDeadlineEnforcingBlobAccess creates a decorator for BlobAccess
// that enforces execution timeouts.
func NewDeadlineEnforcingBlobAccess(delegate BlobAccess, timeout time.Duration) BlobAccess {
	return &deadlineEnforcingBlobAccess{
		delegate: delegate,
		timeout:  timeout,
	}
}

func (d *deadlineEnforcingBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	return d.delegate.GetCapabilities(ctxWithTimeout, instanceName)
}

func (d *deadlineEnforcingBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)

	return buffer.WithErrorHandler(
		d.delegate.Get(ctxWithTimeout, digest),
		&contextCancelingErrorHandler{cancel: cancel},
	)
}

func (d *deadlineEnforcingBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)

	return buffer.WithErrorHandler(
		d.delegate.GetFromComposite(ctxWithTimeout, parentDigest, childDigest, slicer),
		&contextCancelingErrorHandler{cancel: cancel},
	)
}

func (d *deadlineEnforcingBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	return d.delegate.Put(ctxWithTimeout, digest, b)
}

func (d *deadlineEnforcingBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	return d.delegate.FindMissing(ctxWithTimeout, digests)
}

// contextCancelingErrorHandler is used by deadlineEnforcingBlobAccess
// to ensure the Context object created by Get() and GetFromComposite()
// is canceled after the buffer is processed. This ensures that in the
// success path any resources allocated by context.WithTimeout() are
// released immediately, instead of waiting for the timeout to be
// reached.
type contextCancelingErrorHandler struct {
	cancel context.CancelFunc
}

func (contextCancelingErrorHandler) OnError(err error) (buffer.Buffer, error) {
	return nil, err
}

func (eh *contextCancelingErrorHandler) Done() {
	eh.cancel()
	eh.cancel = nil
}
