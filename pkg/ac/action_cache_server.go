package ac

import (
	"context"
	"fmt"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type actionCacheServer struct {
	actionCache              ActionCache
	blobAccess               blobstore.BlobAccess
	allowUpdatesForInstances map[string]bool
}

// NewActionCacheServer creates a GRPC service for serving the contents
// of a Bazel Action Cache (AC) to Bazel.
func NewActionCacheServer(actionCache ActionCache, blobAccess blobstore.BlobAccess, allowUpdatesForInstances map[string]bool) remoteexecution.ActionCacheServer {
	return &actionCacheServer{
		actionCache:              actionCache,
		blobAccess:               blobAccess,
		allowUpdatesForInstances: allowUpdatesForInstances,
	}
}

func (s *actionCacheServer) GetActionResult(ctx context.Context, in *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return nil, err
	}

	actionResult, err := s.actionCache.GetActionResult(ctx, digest)
	if err != nil {
		return nil, err
	}

	var digests []*util.Digest
	for _, output := range actionResult.OutputFiles {
		if output.Digest != nil {
			OutputDigest, err := util.NewDigest(in.InstanceName, output.Digest)

			if err != nil {
				fmt.Println(err)
			} else {
				digests = append(digests, OutputDigest)
			}
		}
	}

	// Check that we have no missing blobs
	if missing, err := s.blobAccess.FindMissing(ctx, digests); missing != nil {
		return nil, status.Errorf(codes.NotFound, "Blob not found")
	}

	return s.actionCache.GetActionResult(ctx, digest)
}

func (s *actionCacheServer) UpdateActionResult(ctx context.Context, in *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return nil, err
	}
	if instance := digest.GetInstance(); !s.allowUpdatesForInstances[instance] {
		return nil, status.Errorf(codes.Unimplemented, "This service can only be used to get action results for instance %#v", instance)
	}
	return in.ActionResult, s.actionCache.PutActionResult(ctx, digest, in.ActionResult)
}
