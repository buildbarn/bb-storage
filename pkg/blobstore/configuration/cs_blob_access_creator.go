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
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/cloud/gcp"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	http_client "github.com/buildbarn/bb-storage/pkg/http/client"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/storage"
)

var csCapabilitiesProvider = capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
	CacheCapabilities: &remoteexecution.CacheCapabilities{
		DigestFunctions: digest.SupportedDigestFunctions,
		// MaxBatchTotalSize: Not used by Bazel yet.
	},
})

type csBlobAccessCreator struct {
	csBlobReplicatorCreator

	maximumMessageSizeBytes int
	zstdPool                bb_zstd.Pool
}

// NewCSBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Chunk Storage.
func NewCSBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool bb_zstd.Pool) BlobAccessCreator {
	return &csBlobAccessCreator{
		csBlobReplicatorCreator: csBlobReplicatorCreator{
			grpcClientFactory: grpcClientFactory,
		},
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		zstdPool:                zstdPool,
	}
}

func (csBlobAccessCreator) GetBaseDigestKeyFormat() digest.KeyFormat {
	return digest.KeyWithoutInstance
}

func (csBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.CASReadBufferFactory
}

func (csBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return csCapabilitiesProvider
}

func (csBlobAccessCreator) NewBlockListGrowthPolicy(currentBlocks, newBlocks int) (local.BlockListGrowthPolicy, error) {
	return local.NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks), nil
}

func (csBlobAccessCreator) NewHierarchicalInstanceNamesLocalBlobAccess(keyLocationMap local.KeyLocationMap, locationBlobMap local.LocationBlobMap, globalLock *sync.RWMutex, capabilitiesProvider capabilities.Provider) (blobstore.BlobAccess, error) {
	return local.NewHierarchicalCASBlobAccess(keyLocationMap, locationBlobMap, globalLock, capabilitiesProvider), nil
}

func (bac *csBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ExistenceCaching:
		base, err := nestedCreator.NewNestedBlobAccess(backend.ExistenceCaching.Backend, bac)
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
		grpc := backend.Grpc
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		var zstdPool bb_zstd.Pool
		if backend.Grpc.EnableCompression {
			zstdPool = bac.zstdPool
		}
		ba := grpcclients.NewCSBlobAccess(client, uuid.NewRandom, 64<<10, zstdPool)
		if grpc.CapabilitiesCache != nil {
			cache, err := blobstore.NewTTLCacheFromConfiguration[*remoteexecution.ServerCapabilities](
				grpc.CapabilitiesCache,
				clock.SystemClock,
				"CSCapabilityCachingBlobStore",
			)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create capabilities cache")
			}
			ba = blobstore.NewCapabilitiesCachingBlobAccess(ba, cache)
		}
		// TODO: Should we provide a configuration option, so
		// that digest.KeyWithoutInstance can be used?
		return BlobAccessInfo{
			BlobAccess:      ba,
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	case *pb.BlobAccessConfiguration_ReferenceExpanding:
		// The backend used by ReferenceExpandingBlobAccess is
		// an Indirect Content Addressable Storage (ICAS). This
		// backend stores Reference messages that point to the
		// location of a blob, not the blobs themselves. Create
		// a new BlobAccessCreator to ensure data is loaded
		// properly.
		indirectContentAddressableStorage, err := nestedCreator.NewNestedBlobAccess(
			backend.ReferenceExpanding.IndirectContentAddressableStorage,
			NewICASBlobAccessCreator(
				bac.grpcClientFactory,
				bac.maximumMessageSizeBytes,
			),
		)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}

		var contentAddressableStorage blobstore.BlobAccess
		if backend.ReferenceExpanding.ContentAddressableStorage != nil {
			backend, err := nestedCreator.NewNestedBlobAccess(backend.ReferenceExpanding.ContentAddressableStorage, bac)
			if err != nil {
				return BlobAccessInfo{}, "", err
			}
			contentAddressableStorage = backend.BlobAccess
		} else {
			contentAddressableStorage = blobstore.NewErrorBlobAccess(status.Error(codes.Unimplemented, "No Content Addressable Storage configured"))
		}

		awsConfig, err := aws.NewConfigFromConfiguration(backend.ReferenceExpanding.AwsSession, "S3ReferenceExpandingBlobAccess")
		if err != nil {
			return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create AWS config")
		}

		roundTripper, err := http_client.NewRoundTripperFromConfiguration(backend.ReferenceExpanding.HttpClient)
		if err != nil {
			return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create HTTP client")
		}

		var gcsClient gcp.StorageClient
		if gcpClientOptions := backend.ReferenceExpanding.GcpClientOptions; gcpClientOptions != nil {
			clientOptions, err := gcp.NewClientOptionsFromConfiguration(gcpClientOptions, "GCSReferenceExpandingBlobAccess")
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create GCP client options")
			}
			client, err := storage.NewClient(context.Background(), clientOptions...)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create GCS client")
			}
			gcsClient = gcp.NewWrappedStorageClient(client)
		}

		return BlobAccessInfo{
			BlobAccess: blobstore.NewReferenceExpandingBlobAccess(
				indirectContentAddressableStorage.BlobAccess,
				contentAddressableStorage,
				&http.Client{
					Transport: http_client.NewMetricsRoundTripper(roundTripper, "HTTPReferenceExpandingBlobAccess"),
				},
				s3.NewFromConfig(awsConfig),
				gcsClient,
				bac.maximumMessageSizeBytes,
				bac.zstdPool,
			),
			DigestKeyFormat: indirectContentAddressableStorage.DigestKeyFormat,
		}, "reference_expanding", nil
	default:
		return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Configuration did not contain a supported storage backend")
	}
}

func (csBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	// For the Content Addressable Storage it is required that the empty
	// blob is always present. This decorator ensures that requests
	// for the empty blob never contact the storage backend.
	// More details: https://github.com/bazelbuild/bazel/issues/11063
	return blobstore.NewEmptyBlobInjectingBlobAccess(blobAccess)
}
