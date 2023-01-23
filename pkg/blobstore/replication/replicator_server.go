package replication

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	replicator_pb "github.com/buildbarn/bb-storage/pkg/proto/replicator"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/protobuf/types/known/emptypb"
)

type replicatorServer struct {
	replicator BlobReplicator
}

// NewReplicatorServer creates a gRPC stub for the Replicator service
// that forwards all calls to BlobReplicator.
func NewReplicatorServer(replicator BlobReplicator) replicator_pb.ReplicatorServer {
	return replicatorServer{
		replicator: replicator,
	}
}

func (rs replicatorServer) ReplicateBlobs(ctx context.Context, request *replicator_pb.ReplicateBlobsRequest) (*emptypb.Empty, error) {
	instanceName, err := digest.NewInstanceName(request.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", request.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, 0)
	if err != nil {
		return nil, err
	}

	digests := digest.NewSetBuilder()
	for i, blobDigest := range request.BlobDigests {
		d, err := digestFunction.NewDigestFromProto(blobDigest)
		if err != nil {
			return nil, util.StatusWrapf(err, "Digest at index %d", i)
		}
		digests.Add(d)
	}
	return &emptypb.Empty{}, rs.replicator.ReplicateMultiple(ctx, digests.Build())
}
