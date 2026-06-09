package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type csBlobReplicatorCreator struct {
	grpcClientFactory grpc.ClientFactory
}

// NewCSBlobReplicatorCreator creates a BlobReplicatorCreator that can
// be provided to NewBlobReplicatorFromConfiguration() to construct a
// BlobReplicator that is suitable for replicating Chunk Storage
// objects.
func NewCSBlobReplicatorCreator(grpcClientFactory grpc.ClientFactory) BlobReplicatorCreator {
	return &csBlobReplicatorCreator{
		grpcClientFactory: grpcClientFactory,
	}
}

func (csBlobReplicatorCreator) GetStorageTypeName() string {
	return "cs"
}

func (brc *csBlobReplicatorCreator) NewCustomBlobReplicator(terminationGroup program.Group, configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error) {
	switch mode := configuration.Mode.(type) {
	case *pb.BlobReplicatorConfiguration_Deduplicating:
		base, err := NewBlobReplicatorFromConfiguration(terminationGroup, mode.Deduplicating, source, sink, brc)
		if err != nil {
			return nil, err
		}
		return replication.NewDeduplicatingBlobReplicator(base, sink.BlobAccess, sink.DigestKeyFormat), nil
	case *pb.BlobReplicatorConfiguration_Remote:
		client, err := brc.grpcClientFactory.NewClientFromConfiguration(mode.Remote, terminationGroup)
		if err != nil {
			return nil, err
		}
		return replication.NewRemoteBlobReplicator(source, client), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
	}
}
