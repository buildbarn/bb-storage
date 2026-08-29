package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/protobuf/proto"
)

type blobAccessMessageReader[T proto.Message] struct {
	blobAccess              BlobAccess
	maximumMessageSizeBytes int
	allocator               func() T
}

// NewBlobAccessMessageReader creates a storage.MessageReader that reads
// a message from a BlobAccess up to maximumMessageSizeBytes large.
// Requires an allocator that returns allocated objects of type T.
func NewBlobAccessMessageReader[T proto.Message](blobAccess BlobAccess, maximumMessageSizeBytes int, allocator func() T) cas.MessageReader[T] {
	return blobAccessMessageReader[T]{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		allocator:               allocator,
	}
}

func (mr blobAccessMessageReader[T]) ReadMessage(ctx context.Context, d digest.Digest) (T, error) {
	var zero T
	ret, err := mr.blobAccess.Get(ctx, d).ToProto(mr.allocator(), mr.maximumMessageSizeBytes)
	if err != nil {
		return zero, err
	}
	return ret.(T), nil
}
