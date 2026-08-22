package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/protobuf/proto"
)

type blobAccessMessageReader[T any, TPtr interface {
	*T
	proto.Message
}] struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewBlobAccessMessageReader creates a MessageReader that reads a
// message from a BlobAccess up to maximumMessageSizeBytes large.
func NewBlobAccessMessageReader[T any, TPtr interface {
	*T
	proto.Message
}](blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) MessageReader[TPtr] {
	return blobAccessMessageReader[T, TPtr]{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (mr blobAccessMessageReader[T, TPtr]) ReadMessage(ctx context.Context, d digest.Digest) (TPtr, error) {
	msg := TPtr(new(T))
	ret, err := mr.blobAccess.Get(ctx, d).ToProto(msg, mr.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return ret.(TPtr), nil
}
