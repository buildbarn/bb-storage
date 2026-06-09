package blobstore

import (
	"context"
	"log"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

type errorBlobAccess[T any] struct {
	err error
}

// NewErrorBlobAccess creates a BlobAccess that returns a fixed error
// response. Such an implementation is useful for adding explicit
// rejection of oversized requests or disabling storage entirely.
func NewErrorBlobAccess[T any](err error) BlobAccess[T] {
	if err == nil {
		log.Fatal("Attempted to create error blob access with nil error")
	}
	return &errorBlobAccess[T]{
		err: err,
	}
}

func (ba *errorBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	var zero T
	return zero, ba.err
}

func (ba *errorBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	return ba.err
}

func (ba *errorBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, ba.err
}

func (ba *errorBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	return nil, ba.err
}
