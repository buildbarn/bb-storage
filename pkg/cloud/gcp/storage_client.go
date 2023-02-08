package gcp

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

// StorageClient contains the methods of the Google Cloud SDK's
// storage.Client type that are used by this code base. This interface
// has been added to permit unit testing.
type StorageClient interface {
	Bucket(name string) StorageBucketHandle
}

type wrappedStorageClient struct {
	impl *storage.Client
}

// NewWrappedStorageClient converts a concrete instance of
// storage.Client to the StorageClient interface, so that it can be used
// in code that can be unit tested.
func NewWrappedStorageClient(impl *storage.Client) StorageClient {
	return wrappedStorageClient{
		impl: impl,
	}
}

func (w wrappedStorageClient) Bucket(name string) StorageBucketHandle {
	return wrappedStorageBucketHandle{
		impl: w.impl.Bucket(name),
	}
}

// StorageBucketHandle contains the methods of the Google Cloud SDK's
// storage.BucketHandle type that are used by this code base. This
// interface has been added to permit unit testing.
type StorageBucketHandle interface {
	Object(name string) StorageObjectHandle
}

type wrappedStorageBucketHandle struct {
	impl *storage.BucketHandle
}

func (w wrappedStorageBucketHandle) Object(name string) StorageObjectHandle {
	return wrappedStorageObjectHandle{
		impl: w.impl.Object(name),
	}
}

// StorageObjectHandle contains the methods of the Google Cloud SDK's
// storage.ObjectHandle type that are used by this code base. This
// interface has been added to permit unit testing.
type StorageObjectHandle interface {
	NewRangeReader(ctx context.Context, offset, length int64) (io.ReadCloser, error)
}

type wrappedStorageObjectHandle struct {
	impl *storage.ObjectHandle
}

// ReadUntilEOF is a value to provide to NewRangeReader()'s length
// argument to request reading the object until the end.
const ReadUntilEOF int64 = -1

func (w wrappedStorageObjectHandle) NewRangeReader(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	return w.impl.NewRangeReader(ctx, offset, length)
}
