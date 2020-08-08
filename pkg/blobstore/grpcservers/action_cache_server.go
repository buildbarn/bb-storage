package grpcservers

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type actionCacheServer struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewActionCacheServer creates a GRPC service for serving the contents
// of a Bazel Action Cache (AC) to Bazel.
func NewActionCacheServer(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) remoteexecution.ActionCacheServer {
	return &actionCacheServer{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (s *actionCacheServer) GetActionResult(ctx context.Context, in *remoteexecution.GetActionResultRequest) (*remoteexecution.ActionResult, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digest, err := instanceName.NewDigestFromProto(in.ActionDigest)
	if err != nil {
		return nil, err
	}
	actionResult, err := s.blobAccess.Get(ctx, digest).ToProto(
		&remoteexecution.ActionResult{},
		s.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return actionResult.(*remoteexecution.ActionResult), nil
}

func (s *actionCacheServer) UpdateActionResult(ctx context.Context, in *remoteexecution.UpdateActionResultRequest) (*remoteexecution.ActionResult, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digest, err := instanceName.NewDigestFromProto(in.ActionDigest)
	if err != nil {
		return nil, err
	}
	return in.ActionResult, s.blobAccess.Put(
		ctx,
		digest,
		buffer.NewProtoBufferFromProto(in.ActionResult, buffer.UserProvided))
}
