package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type protoBlobReplicatorCreator[T any] struct{}

func (protoBlobReplicatorCreator[T]) NewCustomBlobReplicator(terminationGroup program.Group, configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess[T], sink BlobAccessInfo[T]) (replication.BlobReplicator, error) {
	return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
}
