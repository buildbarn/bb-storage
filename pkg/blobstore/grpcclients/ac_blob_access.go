package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type acBlobAccess struct {
	actionCacheClient       remoteexecution.ActionCacheClient
	maximumMessageSizeBytes int
}

// NewACBlobAccess creates a BlobAccess handle that relays any requests
// to a GRPC service that implements the remoteexecution.ActionCache
// service. That is the service that Bazel uses to access action results
// stored in the Action Cache.
func NewACBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &acBlobAccess{
		actionCacheClient:       remoteexecution.NewActionCacheClient(client),
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *acBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	actionResult, err := ba.actionCacheClient.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
		InstanceName: digest.GetInstance(),
		ActionDigest: digest.GetPartialDigest(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewProtoBufferFromProto(actionResult, buffer.Irreparable)
}

func (ba *acBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}
	_, err = ba.actionCacheClient.UpdateActionResult(ctx, &remoteexecution.UpdateActionResultRequest{
		InstanceName: digest.GetInstance(),
		ActionDigest: digest.GetPartialDigest(),
		ActionResult: actionResult.(*remoteexecution.ActionResult),
	})
	return err
}

func (ba *acBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "Bazel action cache does not support bulk existence checking")
}
