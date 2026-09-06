package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MessageReader can be used to read a parsed proto.Message from the
// Content Addressable Storage.
type MessageReader[T proto.Message] interface {
	// Return a parsed message from storage.
	ReadMessage(ctx context.Context, d digest.Digest) (T, error)
}

type messageReader[T any, TPtr interface {
	*T
	proto.Message
}] struct {
	contentAddressableStorage ContentAddressableStorage
	maximumMessageSizeBytes   int
}

// NewMessageReader creates a creates a MessageReader that reads a
// message from a BlobAccess up to maximumMessageSizeBytes large.
func NewMessageReader[T any, TPtr interface {
	*T
	proto.Message
}](contentAddressableStorage ContentAddressableStorage, maximumMessageSizeBytes int) MessageReader[TPtr] {
	return &messageReader[T, TPtr]{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (mr *messageReader[T, TPtr]) ReadMessage(ctx context.Context, d digest.Digest) (TPtr, error) {
	if d.GetSizeBytes() > int64(mr.maximumMessageSizeBytes) {
		return nil, status.Errorf(codes.InvalidArgument, "Digest size of %d bytes exceeds maximum message size of %d bytes", d.GetSizeBytes(), mr.maximumMessageSizeBytes)
	}
	msg := TPtr(new(T))
	bytes, err := GetBytes(ctx, mr.contentAddressableStorage, d)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(bytes, msg)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to unmarshal message")
	}
	return msg, nil
}
