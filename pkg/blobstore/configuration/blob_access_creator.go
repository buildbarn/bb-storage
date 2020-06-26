package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

// BlobAccessCreator contains a set of methods that are invoked by the
// generic NewBlobAccessFromConfiguration() function to create a
// BlobAccess of a specific kind (e.g., Action Cache, Content
// Addressable Storage).
type BlobAccessCreator interface {
	BlobReplicatorCreator

	// GetStorageType() returns operations that can be used by
	// BlobAccess to create Buffer objects to return data.
	GetStorageType() blobstore.StorageType
	// GetStorageTypeName() returns a short string that identifies
	// the purpose of this storage (e.g., "ac", "cas").
	GetStorageTypeName() string
	// NewCustomBlobAccess() can be used as a fallback to create
	// BlobAccess instances that only apply to this storage type.
	// For example, CompletenessCheckingBlobAccess is only
	// applicable to the Action Cache.
	NewCustomBlobAccess(configuration *pb.BlobAccessConfiguration) (blobstore.BlobAccess, string, error)
	// WrapTopLevelBlobAccess() is called at the very end of
	// NewBlobAccessFromConfiguration() to apply any top-level
	// decorators.
	WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess
}
