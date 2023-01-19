package grpcservers

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/emptypb"
)

type initialSizeClassCacheServer struct {
	blobAccess              blobstore.BlobAccess
	maximumMessageSizeBytes int
}

// NewInitialSizeClassCacheServer creates a gRPC service for serving the
// contents of an Initial Size Class Cache (ISCC). The ISCC is a
// Buildbarn specific extension for letting the scheduler store
// execution times of actions, so that it can make better predictions
// about which size class to pick during future invocations of similar
// actions.
func NewInitialSizeClassCacheServer(blobAccess blobstore.BlobAccess, maximumMessageSizeBytes int) iscc.InitialSizeClassCacheServer {
	return &initialSizeClassCacheServer{
		blobAccess:              blobAccess,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (s *initialSizeClassCacheServer) GetPreviousExecutionStats(ctx context.Context, in *iscc.GetPreviousExecutionStatsRequest) (*iscc.PreviousExecutionStats, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.ReducedActionDigest.GetHash()))
	if err != nil {
		return nil, err
	}

	digest, err := digestFunction.NewDigestFromProto(in.ReducedActionDigest)
	if err != nil {
		return nil, err
	}
	previousExecutionStats, err := s.blobAccess.Get(ctx, digest).ToProto(
		&iscc.PreviousExecutionStats{},
		s.maximumMessageSizeBytes)
	if err != nil {
		return nil, err
	}
	return previousExecutionStats.(*iscc.PreviousExecutionStats), nil
}

func (s *initialSizeClassCacheServer) UpdatePreviousExecutionStats(ctx context.Context, in *iscc.UpdatePreviousExecutionStatsRequest) (*emptypb.Empty, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.ReducedActionDigest.GetHash()))
	if err != nil {
		return nil, err
	}

	digest, err := digestFunction.NewDigestFromProto(in.ReducedActionDigest)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, s.blobAccess.Put(
		ctx,
		digest,
		buffer.NewProtoBufferFromProto(in.PreviousExecutionStats, buffer.UserProvided))
}
