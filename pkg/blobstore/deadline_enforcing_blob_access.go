package blobstore

import (
	"context"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type deadlineEnforcingBlobAccess[T any] struct {
	delegate BlobAccess[T]
	timeout  time.Duration
}

// NewDeadlineEnforcingBlobAccess creates a decorator for BlobAccess
// that enforces execution timeouts.
func NewDeadlineEnforcingBlobAccess[T any](delegate BlobAccess[T], timeout time.Duration) BlobAccess[T] {
	return &deadlineEnforcingBlobAccess[T]{
		delegate: delegate,
		timeout:  timeout,
	}
}

func (d *deadlineEnforcingBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.delegate.GetCapabilities(ctxWithTimeout, instanceName)
}

func (d *deadlineEnforcingBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.delegate.Get(ctxWithTimeout, digest)
}

func (d *deadlineEnforcingBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.delegate.Put(ctxWithTimeout, digest, value)
}

func (d *deadlineEnforcingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	return d.delegate.FindMissing(ctxWithTimeout, digests)
}
