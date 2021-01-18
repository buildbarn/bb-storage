package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type casBlobReplicatorCreator struct {
	grpcClientFactory grpc.ClientFactory
}

// NewCASBlobReplicatorCreator creates a BlobReplicatorCreator that can
// be provided to NewBlobReplicatorFromConfiguration() to construct a
// BlobReplicator that is suitable for replicating Content Addressable
// Storage objects.
func NewCASBlobReplicatorCreator(grpcClientFactory grpc.ClientFactory) BlobReplicatorCreator {
	return &casBlobReplicatorCreator{
		grpcClientFactory: grpcClientFactory,
	}
}

func (brc *casBlobReplicatorCreator) NewCustomBlobReplicator(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error) {
	switch mode := configuration.Mode.(type) {
	case *pb.BlobReplicatorConfiguration_Grpc:
		client, err := brc.grpcClientFactory.NewClientFromConfiguration(mode.Grpc)
		if err != nil {
			return nil, err
		}
		return replication.NewGrpcBlobReplicator(source, client), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
	}
}
