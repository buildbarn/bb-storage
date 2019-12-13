package ac

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionCacheServer struct {
	blobAccess               blobstore.BlobAccess
	allowUpdatesForInstances map[string]bool
	maximumMessageSizeBytes  int
}

// NewActionCacheServer creates a GRPC service for serving the contents
// of a Bazel Action Cache (AC) to Bazel.
func NewActionCacheServer(blobAccess blobstore.BlobAccess, allowUpdatesForInstances map[string]bool, maximumMessageSizeBytes int) remoteexecution.ActionCacheServer {
	return &actionCacheServer{
		blobAccess:               blobAccess,
		allowUpdatesForInstances: allowUpdatesForInstances,
		maximumMessageSizeBytes:  maximumMessageSizeBytes,
	}
}

func (s *actionCacheServer) GetActionResult(ctx context.Context, in *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return nil, err
	}
	return s.blobAccess.Get(ctx, digest).ToActionResult(s.maximumMessageSizeBytes)
}

func (s *actionCacheServer) UpdateActionResult(ctx context.Context, in *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return nil, err
	}
	if instance := digest.GetInstance(); !s.allowUpdatesForInstances[instance] {
		return nil, status.Errorf(codes.Unimplemented, "This service can only be used to get action results for instance %#v", instance)
	}
	return in.ActionResult, s.blobAccess.Put(
		ctx,
		digest,
		buffer.NewACBufferFromActionResult(in.ActionResult, buffer.UserProvided))
}
