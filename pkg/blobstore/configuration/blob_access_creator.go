package configuration

import (
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

// BlobAccessCreator contains a set of methods that are invoked by the
// generic NewBlobAccessFromConfiguration() function to create a
// BlobAccess of a specific kind (e.g., Action Cache, Content
// Addressable Storage).
type BlobAccessCreator interface {
	BlobReplicatorCreator

	// GetBaseDigestKeyFormat() returns the format that leaf
	// instances of BlobAccess (e.g., LocalBlobAccess) should be
	// used to compute keys of digests.
	//
	// For the Content Addressable Storage (CAS), this function may
	// return digest.KeyWithoutInstance, so that identical objects
	// are only stored once.
	GetBaseDigestKeyFormat() digest.KeyFormat
	// GetReadBufferFactory() returns operations that can be used by
	// BlobAccess to create Buffer objects to return data.
	GetReadBufferFactory() blobstore.ReadBufferFactory
	// GetStorageTypeName() returns a short string that identifies
	// the purpose of this storage (e.g., "ac", "cas").
	GetStorageTypeName() string
	// GetCapabilitiesProvider() returns a provider of REv2
	// ServerCapabilities messages that should be returned for
	// backends that can't report their own capabilities. This
	// provider returns sane default values.
	GetDefaultCapabilitiesProvider() capabilities.Provider
	// NewBlockListGrowthPolicy() creates a BlockListGrowthPolicy
	// for LocalBlobAccess that is recommended for this storage type.
	NewBlockListGrowthPolicy(currentBlocks, newBlocks int) (local.BlockListGrowthPolicy, error)
	// NewHierarchicalInstanceNamesLocalBlobAccess() creates a
	// BlobAccess suitable for storing data on the local system that
	// uses hierarchical instance names.
	NewHierarchicalInstanceNamesLocalBlobAccess(keyLocationMap local.KeyLocationMap, locationBlobMap local.LocationBlobMap, globalLock *sync.RWMutex) (blobstore.BlobAccess, error)
	// NewCustomBlobAccess() can be used as a fallback to create
	// BlobAccess instances that only apply to this storage type.
	// For example, CompletenessCheckingBlobAccess is only
	// applicable to the Action Cache.
	NewCustomBlobAccess(configuration *pb.BlobAccessConfiguration) (BlobAccessInfo, string, error)
	// WrapTopLevelBlobAccess() is called at the very end of
	// NewBlobAccessFromConfiguration() to apply any top-level
	// decorators.
	WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess
}
