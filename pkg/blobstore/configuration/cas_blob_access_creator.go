package configuration

import (
	"context"
	"net/http"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	bb_http "github.com/buildbarn/bb-storage/pkg/http"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var casCapabilitiesProvider = capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
	CacheCapabilities: &remoteexecution.CacheCapabilities{
		DigestFunctions: digest.SupportedDigestFunctions,
		// MaxBatchTotalSize: Not used by Bazel yet.
	},
})

type casBlobAccessCreator struct {
	casBlobReplicatorCreator

	maximumMessageSizeBytes int
}

// NewCASBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Content Addressable
// Storage.
func NewCASBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &casBlobAccessCreator{
		casBlobReplicatorCreator: casBlobReplicatorCreator{
			grpcClientFactory: grpcClientFactory,
		},
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (bac *casBlobAccessCreator) GetBaseDigestKeyFormat() digest.KeyFormat {
	return digest.KeyWithoutInstance
}

func (bac *casBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.CASReadBufferFactory
}

func (bac *casBlobAccessCreator) GetStorageTypeName() string {
	return "cas"
}

func (bac *casBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return casCapabilitiesProvider
}

func (bac *casBlobAccessCreator) NewBlockListGrowthPolicy(currentBlocks, newBlocks int) (local.BlockListGrowthPolicy, error) {
	return local.NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks), nil
}

func (bac *casBlobAccessCreator) NewHierarchicalInstanceNamesLocalBlobAccess(keyLocationMap local.KeyLocationMap, locationBlobMap local.LocationBlobMap, globalLock *sync.RWMutex) (blobstore.BlobAccess, error) {
	return local.NewHierarchicalCASBlobAccess(keyLocationMap, locationBlobMap, globalLock, casCapabilitiesProvider), nil
}

func (bac *casBlobAccessCreator) NewCustomBlobAccess(terminationContext context.Context, terminationGroup *sync.WaitGroup, configuration *pb.BlobAccessConfiguration) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ExistenceCaching:
		base, err := NewNestedBlobAccess(terminationContext, terminationGroup, backend.ExistenceCaching.Backend, bac)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		existenceCache, err := digest.NewExistenceCacheFromConfiguration(backend.ExistenceCaching.ExistenceCache, base.DigestKeyFormat, "ExistenceCachingBlobAccess")
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      blobstore.NewExistenceCachingBlobAccess(base.BlobAccess, existenceCache),
			DigestKeyFormat: base.DigestKeyFormat,
		}, "existence_caching", nil
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		// TODO: Should we provide a configuration option, so
		// that digest.KeyWithoutInstance can be used?
		return BlobAccessInfo{
			BlobAccess:      grpcclients.NewCASBlobAccess(client, uuid.NewRandom, 65536),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	case *pb.BlobAccessConfiguration_ReferenceExpanding:
		// The backend used by ReferenceExpandingBlobAccess is
		// an Indirect Content Addressable Storage (ICAS). This
		// backend stores Reference messages that point to the
		// location of a blob, not the blobs themselves. Create
		// a new BlobAccessCreator to ensure data is loaded
		// properly.
		base, err := NewNestedBlobAccess(
			terminationContext,
			terminationGroup,
			backend.ReferenceExpanding.IndirectContentAddressableStorage,
			NewICASBlobAccessCreator(
				bac.grpcClientFactory,
				bac.maximumMessageSizeBytes))
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		cfg, err := aws.NewConfigFromConfiguration(backend.ReferenceExpanding.AwsSession, "S3ReferenceExpandingBlobAccess")
		if err != nil {
			return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create AWS config")
		}
		roundTripper, err := bb_http.NewRoundTripperFromConfiguration(backend.ReferenceExpanding.HttpClient)
		if err != nil {
			return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create HTTP client")
		}
		return BlobAccessInfo{
			BlobAccess: blobstore.NewReferenceExpandingBlobAccess(
				base.BlobAccess,
				&http.Client{
					Transport: bb_http.NewMetricsRoundTripper(roundTripper, "HTTPReferenceExpandingBlobAccess"),
				},
				s3.NewFromConfig(cfg),
				bac.maximumMessageSizeBytes),
			DigestKeyFormat: base.DigestKeyFormat,
		}, "reference_expanding", nil
	default:
		return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Configuration did not contain a supported storage backend")
	}
}

func (bac *casBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	// For the Content Addressable Storage it is required that the empty
	// blob is always present. This decorator ensures that requests
	// for the empty blob never contact the storage backend.
	// More details: https://github.com/bazelbuild/bazel/issues/11063
	return blobstore.NewEmptyBlobInjectingBlobAccess(blobAccess)
}
