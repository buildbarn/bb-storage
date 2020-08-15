package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type acBlobReplicatorCreator struct{}

func (brc acBlobReplicatorCreator) NewCustomBlobReplicator(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error) {
	return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
}

// ACBlobReplicatorCreator is a BlobReplicatorCreator that can be
// provided to NewBlobReplicatorFromConfiguration() to construct a
// BlobReplicator that is suitable for replicating Action Cache objects.
var ACBlobReplicatorCreator BlobReplicatorCreator = acBlobReplicatorCreator{}
