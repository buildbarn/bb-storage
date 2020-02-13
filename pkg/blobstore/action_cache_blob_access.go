package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.opencensus.io/trace"
)

type actionCacheBlobAccess struct {
	actionCacheClient       remoteexecution.ActionCacheClient
	maximumMessageSizeBytes int
}

// NewActionCacheBlobAccess creates a BlobAccess handle that relays any
// requests to a GRPC service that implements the
// remoteexecution.ActionCache service. That is the service that Bazel
// uses to access action results stored in the Action Cache.
func NewActionCacheBlobAccess(client *grpc.ClientConn, maximumMessageSizeBytes int) BlobAccess {
	return &actionCacheBlobAccess{
		actionCacheClient:       remoteexecution.NewActionCacheClient(client),
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *actionCacheBlobAccess) Get(ctx context.Context, digest *util.Digest) buffer.Buffer {
	ctx, span := trace.StartSpan(ctx, "blobstore.ActionCacheBlobAccess.Get")
	defer span.End()

	actionResult, err := ba.actionCacheClient.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
		InstanceName: digest.GetInstance(),
		ActionDigest: digest.GetPartialDigest(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewACBufferFromActionResult(actionResult, buffer.Irreparable)
}

func (ba *actionCacheBlobAccess) Put(ctx context.Context, digest *util.Digest, b buffer.Buffer) error {
	ctx, span := trace.StartSpan(ctx, "blobstore.ActionCacheBlobAccess.Put")
	span.AddAttributes(
		trace.StringAttribute("instance-name", digest.GetInstance()),
		trace.StringAttribute("digest-hash", digest.GetHashString()),
	)
	defer span.End()

	actionResult, err := b.ToActionResult(ba.maximumMessageSizeBytes)
	if err != nil {
		span.SetStatus(trace.Status{Code: 1, Message: err.Error()})
		return err
	}
	_, err = ba.actionCacheClient.UpdateActionResult(ctx, &remoteexecution.UpdateActionResultRequest{
		InstanceName: digest.GetInstance(),
		ActionDigest: digest.GetPartialDigest(),
		ActionResult: actionResult,
	})
	return err
}

func (ba *actionCacheBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	return nil, status.Error(codes.Unimplemented, "Bazel action cache does not support bulk existence checking")
}
