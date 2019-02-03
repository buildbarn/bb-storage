package ac

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionCacheServer struct {
	actionCache  ActionCache
	allowUpdates bool
}

// NewActionCacheServer creates a GRPC service for serving the contents
// of a Bazel Action Cache (AC) to Bazel.
func NewActionCacheServer(actionCache ActionCache, allowUpdates bool) remoteexecution.ActionCacheServer {
	return &actionCacheServer{
		actionCache:  actionCache,
		allowUpdates: allowUpdates,
	}
}

func (s *actionCacheServer) GetActionResult(ctx context.Context, in *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return nil, err
	}
	return s.actionCache.GetActionResult(ctx, digest)
}

func (s *actionCacheServer) UpdateActionResult(ctx context.Context, in *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	if !s.allowUpdates {
		return nil, status.Error(codes.Unimplemented, "This service can only be used to get action results")
	}
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return nil, err
	}
	return in.ActionResult, s.actionCache.PutActionResult(ctx, digest, in.ActionResult)
}
