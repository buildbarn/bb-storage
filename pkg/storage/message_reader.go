package storage

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

// MessageReader can be used to read a proto.Message from storage and
// return its contents.
type MessageReader[T proto.Message] interface {
	// Return a parsed message from storage.
	//
	// Implementations may use the provided message to store the
	// unmarshalled message.
	ReadMessage(ctx context.Context, d digest.Digest, msg T) (T, error)
}
