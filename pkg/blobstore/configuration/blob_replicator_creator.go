package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

// BlobReplicatorCreator contains a set of methods that are invoked by
// the generic NewBlobReplicatorFromConfiguration() function to create a
// BlobReplicator of a specific kind (e.g., Action Cache, Content
// Addressable Storage).
type BlobReplicatorCreator interface {
	// NewCustomBlobReplicator() can be used as a fallback to create
	// BlobReplicator instances that only apply to this storage
	// type. For example, sending replication requests over gRPC is
	// only supported for the Content Addressable Storage.
	NewCustomBlobReplicator(terminationGroup program.Group, configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink BlobAccessInfo) (replication.BlobReplicator, error)

	// GetStorageTypeName returns the name of the storage type that
	// this BlobReplicatorCreator is able to create BlobReplicators for.
	GetStorageTypeName() string
}
