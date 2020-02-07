package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateBlobReplicatorFromConfig creates a BlobReplicator object based
// on a configuration file.
func CreateBlobReplicatorFromConfig(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink blobstore.BlobAccess, keyFormat digest.KeyFormat) (mirrored.BlobReplicator, error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "Replicator configuration not specified")
	}
	switch mode := configuration.Mode.(type) {
	case *pb.BlobReplicatorConfiguration_Local:
		return mirrored.NewLocalBlobReplicator(source, sink), nil
	case *pb.BlobReplicatorConfiguration_Remote:
		client, err := bb_grpc.NewGRPCClientFromConfiguration(mode.Remote)
		if err != nil {
			return nil, err
		}
		return mirrored.NewRemoteBlobReplicator(source, client), nil
	case *pb.BlobReplicatorConfiguration_Queued:
		base, err := CreateBlobReplicatorFromConfig(mode.Queued.Base, source, sink, keyFormat)
		if err != nil {
			return nil, err
		}
		existenceCache, err := digest.NewExistenceCacheFromConfiguration(mode.Queued.ExistenceCache, keyFormat, "QueuedBlobReplicator")
		if err != nil {
			return nil, err
		}
		return mirrored.NewQueuedBlobReplicator(source, base, existenceCache), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
	}
}
