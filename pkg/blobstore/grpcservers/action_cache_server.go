package grpcservers

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type actionCacheServer struct {
	blobAccess blobstore.BlobAccess[*remoteexecution.ActionResult]
}

// NewActionCacheServer creates a GRPC service for serving the contents
// of a Bazel Action Cache (AC) to Bazel.
func NewActionCacheServer(blobAccess blobstore.BlobAccess[*remoteexecution.ActionResult]) remoteexecution.ActionCacheServer {
	return &actionCacheServer{
		blobAccess: blobAccess,
	}
}

func (s *actionCacheServer) GetActionResult(ctx context.Context, in *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.ActionDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	digest, err := digestFunction.NewDigestFromProto(in.ActionDigest)
	if err != nil {
		return nil, err
	}
	return s.blobAccess.Get(ctx, digest)
}

func (s *actionCacheServer) UpdateActionResult(ctx context.Context, in *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.ActionDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	digest, err := digestFunction.NewDigestFromProto(in.ActionDigest)
	if err != nil {
		return nil, err
	}
	return in.ActionResult, s.blobAccess.Put(
		ctx,
		digest,
		in.ActionResult,
	)
}
