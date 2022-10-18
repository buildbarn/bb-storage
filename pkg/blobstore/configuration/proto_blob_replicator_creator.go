package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type protoBlobReplicatorCreator struct{}

func (brc protoBlobReplicatorCreator) NewCustomBlobReplicator(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error) {
	return nil, status.Error(codes.InvalidArgument, "Configuration did not contain a supported replicator")
}

var (
	// ACBlobReplicatorCreator is a BlobReplicatorCreator that can be
	// provided to NewBlobReplicatorFromConfiguration() to construct a
	// BlobReplicator that is suitable for replicating Action Cache objects.
	ACBlobReplicatorCreator BlobReplicatorCreator = protoBlobReplicatorCreator{}
	// ICASBlobReplicatorCreator is a BlobReplicatorCreator that can be
	// provided to NewBlobReplicatorFromConfiguration() to construct a
	// BlobReplicator that is suitable for replicating Indirect Content
	// Addressable Storage objects.
	ICASBlobReplicatorCreator BlobReplicatorCreator = protoBlobReplicatorCreator{}
	// ISCCBlobReplicatorCreator is a BlobReplicatorCreator that can
	// be provided to NewBlobReplicatorFromConfiguration() to
	// construct a BlobReplicator that is suitable for replicating
	// Initial Size Class Cache objects.
	ISCCBlobReplicatorCreator BlobReplicatorCreator = protoBlobReplicatorCreator{}
)
