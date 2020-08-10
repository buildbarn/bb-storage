package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewBlobReplicatorFromConfiguration creates a BlobReplicator object
// based on a configuration file.
func NewBlobReplicatorFromConfiguration(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink blobstore.BlobAccess, creator BlobReplicatorCreator) (replication.BlobReplicator, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "Replicator configuration not specified")
	}
	switch mode := configuration.Mode.(type) {
	case *pb.BlobReplicatorConfiguration_Local:
		return replication.NewLocalBlobReplicator(source, sink), nil
	case *pb.BlobReplicatorConfiguration_Noop:
		return replication.NewNoopBlobReplicator(source), nil
	case *pb.BlobReplicatorConfiguration_Queued:
		base, err := NewBlobReplicatorFromConfiguration(mode.Queued.Base, source, sink, creator)
		if err != nil {
			return nil, err
		}
		existenceCache, err := digest.NewExistenceCacheFromConfiguration(mode.Queued.ExistenceCache, creator.GetDigestKeyFormat(), "QueuedBlobReplicator")
		if err != nil {
			return nil, err
		}
		return replication.NewQueuedBlobReplicator(source, base, existenceCache), nil
	default:
		return creator.NewCustomBlobReplicator(configuration, source, sink)
	}
}
