package mirrored

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	replicator_pb "github.com/buildbarn/bb-storage/pkg/proto/replicator"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/protobuf/ptypes/empty"
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

func (rs replicatorServer) ReplicateBlobs(ctx context.Context, request *replicator_pb.ReplicateBlobsRequest) (*empty.Empty, error) {
	digests := digest.NewSetBuilder()
	for i, blobDigest := range request.BlobDigests {
		d, err := digest.NewDigestFromPartialDigest(request.InstanceName, blobDigest)
		if err != nil {
			return nil, util.StatusWrapf(err, "Digest at index %d", i)
		}
		digests.Add(d)
	}
	return &empty.Empty{}, rs.replicator.ReplicateMultiple(ctx, digests.Build())
}
