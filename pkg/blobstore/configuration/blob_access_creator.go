package configuration

import (
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

// NestedBlobAccessCreator is a helper type that implementations of
// BlobAccessCreator may use to construct nested instances of
// BlobAccess. For example, ACBlobAccessCreator will call into this
// interface to create the backend of CompletenessCheckingBlobAccess.
type NestedBlobAccessCreator[T any] interface {
	NewNestedBlobAccess(configuration *pb.BlobAccessConfiguration, creator BlobAccessCreator[T]) (BlobAccessInfo[T], error)
}

// BlobAccessCreator contains a set of methods that are invoked by the
// generic NewBlobAccessFromConfiguration() function to create a
// BlobAccess of a specific kind (e.g., Action Cache, Content
// Addressable Storage).
type BlobAccessCreator[T any] interface {
	BlobReplicatorCreator[T]

	// GetBaseDigestKeyFormat() returns the format that leaf
	// instances of BlobAccess (e.g., LocalBlobAccess) should be
	// used to compute keys of digests.
	//
	// For the Content Addressable Storage (CAS), this function may
	// return digest.KeyWithoutInstance, so that identical objects
	// are only stored once.
	GetBaseDigestKeyFormat() digest.KeyFormat
	// GetBinaryCoder() returns a coder.Coder that Encodes or
	// Decodes a T into a byte slice.
	GetBinaryCoder() coder.Coder[T, []byte]
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
	NewHierarchicalInstanceNamesLocalBlobAccess(keyLocationMap local.KeyLocationMap, blockReferenceResolver local.BlockReferenceResolver, locationBlobMap local.LocationBlobMap, globalLock *sync.RWMutex, capabilitiesProvider capabilities.Provider) (blobstore.BlobAccess[T], error)
	// NewCustomBlobAccess() can be used as a fallback to create
	// BlobAccess instances that only apply to this storage type.
	// For example, CompletenessCheckingBlobAccess is only
	// applicable to the Action Cache.
	NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[T]) (BlobAccessInfo[T], string, error)
	// WrapTopLevelBlobAccess() is called at the very end of
	// NewBlobAccessFromConfiguration() to apply any top-level
	// decorators.
	WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[T]) blobstore.BlobAccess[T]
}
