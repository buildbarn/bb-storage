package cdc

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/protobuf/proto"
)

// PutProto is a helper function for storing Protobuf messages in the
// Content Addressable Storage (CAS). It computes the digest of the
// message and stores it under that key. The digest is then returned, so
// that the object may be referenced.
func PutProto(ctx context.Context, contentAddressableStorage ContentAddressableStorage, message proto.Message, digestFunction digest.Function) (digest.Digest, error) {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}
	digestGenerator := digestFunction.NewGenerator(int64(len(bytes)))
	if _, err := digestGenerator.Write(bytes); err != nil {
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()
	if err := PutBytes(ctx, contentAddressableStorage, blobDigest, bytes); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}
