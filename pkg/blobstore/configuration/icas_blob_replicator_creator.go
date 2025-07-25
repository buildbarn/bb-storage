package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type icasBlobReplicatorCreator struct{}

func (brc icasBlobReplicatorCreator) NewCustomBlobReplicator(terminationGroup program.Group, configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error) {
	switch mode := configuration.Mode.(type) {
	case *pb.BlobReplicatorConfiguration_Deduplicating:
		base, err := NewBlobReplicatorFromConfiguration(terminationGroup, mode.Deduplicating, source, sink, brc)
		if err != nil {
			return nil, err
		}
		return replication.NewDeduplicatingBlobReplicator(base, sink.BlobAccess, sink.DigestKeyFormat), nil
	default:
		return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
	}
}

func (brc icasBlobReplicatorCreator) GetStorageTypeName() string {
	return "icas"
}
