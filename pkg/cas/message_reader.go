package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

// MessageReader can be used to read a parsed proto.Message from the
// Content Addressable Storage.
type MessageReader[T proto.Message] interface {
	// Return a parsed message from storage.
	ReadMessage(ctx context.Context, d digest.Digest) (T, error)
}
