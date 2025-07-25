package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type protoBlobReplicatorCreator struct{}

func (brc protoBlobReplicatorCreator) NewCustomBlobReplicator(terminationGroup program.Group, configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error) {
	return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
}
