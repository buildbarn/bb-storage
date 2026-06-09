package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/protobuf/proto"
)

// PutProto is a helper function for storing Protobuf messages in the
// Content Addressable Storage (CAS). It computes the digest of the
// message and stores it under that key. The digest is then returned, so
// that the object may be referenced.
func PutProto(ctx context.Context, zstdPool zstd.Pool, chunkStorage blobstore.BlobAccess[*buffer.Chunk], chunkListStorage blobstore.BlobAccess[chunklist.ChunkList], params *remoteexecution.RepMaxCdcParams, message proto.Message, digestFunction digest.Function) (digest.Digest, error) {
	bytes, err := proto.Marshal(message)
	if err != nil {
		return digest.BadDigest, err
	}
	digestGenerator := digestFunction.NewGenerator(int64(len(bytes)))
	if _, err := digestGenerator.Write(bytes); err != nil {
		return digest.BadDigest, err
	}
	blobDigest := digestGenerator.Sum()
	if err := PutBytes(ctx, zstdPool, chunkStorage, chunkListStorage, params, blobDigest, bytes); err != nil {
		return digest.BadDigest, err
	}
	return blobDigest, nil
}
