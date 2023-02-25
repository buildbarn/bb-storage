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
	case *pb.BlobReplicatorConfiguration_Deduplicating:
		base, err := NewBlobReplicatorFromConfiguration(mode.Deduplicating, source, sink, brc)
		if err != nil {
			return nil, err
		}
		return replication.NewDeduplicatingBlobReplicator(base, sink.BlobAccess, sink.DigestKeyFormat), nil
	case *pb.BlobReplicatorConfiguration_Remote:
		client, err := brc.grpcClientFactory.NewClientFromConfiguration(mode.Remote)
		if err != nil {
			return nil, err
		}
		return replication.NewRemoteBlobReplicator(source, client), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
	}
}
