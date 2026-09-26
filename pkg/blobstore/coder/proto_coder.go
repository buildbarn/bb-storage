package coder

import (
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/protobuf/proto"
)

type protoCoder[T any, TPtr interface {
	*T
	proto.Message
}] struct{}

// NewProtoCoder returns a Coder that can encode and decode a
// proto.Message into a binary format.
func NewProtoCoder[T any, TPtr interface {
	*T
	proto.Message
}]() Coder[TPtr, []byte] {
	return &protoCoder[T, TPtr]{}
}

func (protoCoder[T, TPtr]) Encode(val TPtr, d digest.Digest) ([]byte, error) {
	return proto.Marshal(val)
}

func (protoCoder[T, TPtr]) Decode(data []byte, d digest.Digest) (TPtr, error) {
	msg := TPtr(new(T))
	err := proto.Unmarshal(data, msg)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to unmarshal message")
	}
	return msg, nil
}
