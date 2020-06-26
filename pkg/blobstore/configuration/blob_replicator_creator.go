package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

// BlobReplicatorCreator contains a set of methods that are invoked by
// the generic NewBlobReplicatorFromConfiguration() function to create a
// BlobReplicator of a specific kind (e.g., Action Cache, Content
// Addressable Storage).
type BlobReplicatorCreator interface {
	// GetDigestKeyFormat() returns the preferred way of creating
	// keys based on digests. For the Content Addressable Storage,
	// it is typically valid to discard the instance name, so that
	// we get a higher cache hit rate in case multiple instance
	// names are used.
	GetDigestKeyFormat() digest.KeyFormat
	// NewCustomBlobReplicator() can be used as a fallback to create
	// BlobReplicator instances that only apply to this storage
	// type. For example, sending replication requests over gRPC is
	// only supported for the Content Addressable Storage.
	NewCustomBlobReplicator(configuration *pb.BlobReplicatorConfiguration, source blobstore.BlobAccess, sink blobstore.BlobAccess) (mirrored.BlobReplicator, error)
}
