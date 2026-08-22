package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/storage"
	"google.golang.org/protobuf/proto"
)

type blobAccessMessageReader[T proto.Message] struct {
	blobAccess              BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessMessageReader creates a storage.MessageReader that reads
// a message from a BlobAccess up to maximumMessageSizeBytes.
func NewBlobAccessMessageReader[T proto.Message](blobAccess BlobAccess, maximumMessageSizeBytes int) storage.MessageReader[T] {
	return blobAccessMessageReader[T]{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (mr blobAccessMessageReader[T]) ReadMessage(ctx context.Context, d digest.Digest, msg T) (T, error) {
	var zero T
	ret, err := mr.blobAccess.Get(ctx, d).ToProto(msg, mr.maximumMessageSizeBytes)
	if err != nil {
		return zero, err
	}
	return ret.(T), nil
}
