package configuration

import (
	"context"
	"net/http"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/blobstore/referenceexpanding"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/cloud/aws"
	"github.com/buildbarn/bb-storage/pkg/cloud/gcp"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	http_client "github.com/buildbarn/bb-storage/pkg/http/client"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/storage"
)

var csCapabilitiesProvider = capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
	CacheCapabilities: &remoteexecution.CacheCapabilities{
		DigestFunctions:   digest.SupportedDigestFunctions,
		SplitBlobSupport:  true,
		SpliceBlobSupport: true,
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
func NewCSBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool bb_zstd.Pool) BlobAccessCreator[*chunk.Chunk] {
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

func (csBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return csCapabilitiesProvider
}

func (bac *csBlobAccessCreator) GetBinaryCoder() coder.Coder[*chunk.Chunk, []byte] {
	return coder.NewChunkCoder(bac.zstdPool)
}

func (csBlobAccessCreator) NewBlockListGrowthPolicy(currentBlocks, newBlocks int) (local.BlockListGrowthPolicy, error) {
	return local.NewImmutableBlockListGrowthPolicy(currentBlocks, newBlocks), nil
}

func (bac *csBlobAccessCreator) NewHierarchicalInstanceNamesLocalBlobAccess(keyLocationMap local.KeyLocationMap, blockReferenceResolver local.BlockReferenceResolver, locationBlobMap local.LocationBlobMap, globalLock *sync.RWMutex, capabilitiesProvider capabilities.Provider) (blobstore.BlobAccess[*chunk.Chunk], error) {
	return local.NewHierarchicalCSBlobAccess(keyLocationMap, blockReferenceResolver, locationBlobMap, globalLock, capabilitiesProvider, bac.GetBinaryCoder()), nil
}

func (bac *csBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[*chunk.Chunk]) (BlobAccessInfo[*chunk.Chunk], string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ExistenceCaching:
		base, err := nestedCreator.NewNestedBlobAccess(backend.ExistenceCaching.Backend, bac)
		if err != nil {
			return BlobAccessInfo[*chunk.Chunk]{}, "", err
		}
		existenceCache, err := digest.NewExistenceCacheFromConfiguration(backend.ExistenceCaching.ExistenceCache, base.DigestKeyFormat, "ExistenceCachingBlobAccess")
		if err != nil {
			return BlobAccessInfo[*chunk.Chunk]{}, "", err
		}
		return BlobAccessInfo[*chunk.Chunk]{
			BlobAccess:      blobstore.NewExistenceCachingBlobAccess(base.BlobAccess, existenceCache),
			DigestKeyFormat: base.DigestKeyFormat,
		}, "existence_caching", nil
	case *pb.BlobAccessConfiguration_Grpc:
		grpc := backend.Grpc
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo[*chunk.Chunk]{}, "", err
		}
		ba := grpcclients.NewCSBlobAccess(client, bac.zstdPool, backend.Grpc.EnableCompression)
		// TODO: Should we provide a configuration option, so
		// that digest.KeyWithoutInstance can be used?
		return BlobAccessInfo[*chunk.Chunk]{
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
		indirectContentAddressableStorage, err := NewBlobAccessFromConfiguration(
			terminationGroup,
			backend.ReferenceExpanding.IndirectContentAddressableStorage,
			NewICASBlobAccessCreator(
				bac.grpcClientFactory,
				bac.maximumMessageSizeBytes,
				bac.zstdPool,
			),
		)
		if err != nil {
			return BlobAccessInfo[*chunk.Chunk]{}, "", err
		}

		var chunkBytesReader reader.Reader[[]byte]
		var chunkListFetcher chunk.ListFetcher
		var cdcParametersFetcher cdc.ParametersFetcher
		if backend.ReferenceExpanding.ContentAddressableStorage != nil {
			chunkBytesReader, _, _, chunkListFetcher, cdcParametersFetcher, _, err = NewCASFromConfiguration(terminationGroup, backend.ReferenceExpanding.ContentAddressableStorage, bac.grpcClientFactory, bac.maximumMessageSizeBytes, bac.zstdPool)
		} else {
			// TODO: is an equivalent of this needed?
			// chunkStorage = blobstore.NewErrorBlobAccess[*chunk.Chunk](status.Error(codes.Unimplemented, "No Content Addressable Storage configured"))
		}

		awsConfig, err := aws.NewConfigFromConfiguration(backend.ReferenceExpanding.AwsSession, "S3ReferenceExpandingBlobAccess")
		if err != nil {
			return BlobAccessInfo[*chunk.Chunk]{}, "", util.StatusWrap(err, "Failed to create AWS config")
		}

		roundTripper, err := http_client.NewRoundTripperFromConfiguration(backend.ReferenceExpanding.HttpClient)
		if err != nil {
			return BlobAccessInfo[*chunk.Chunk]{}, "", util.StatusWrap(err, "Failed to create HTTP client")
		}

		var gcsClient gcp.StorageClient
		if gcpClientOptions := backend.ReferenceExpanding.GcpClientOptions; gcpClientOptions != nil {
			clientOptions, err := gcp.NewClientOptionsFromConfiguration(gcpClientOptions, "GCSReferenceExpandingBlobAccess")
			if err != nil {
				return BlobAccessInfo[*chunk.Chunk]{}, "", util.StatusWrap(err, "Failed to create GCP client options")
			}
			client, err := storage.NewClient(context.Background(), clientOptions...)
			if err != nil {
				return BlobAccessInfo[*chunk.Chunk]{}, "", util.StatusWrap(err, "Failed to create GCS client")
			}
			gcsClient = gcp.NewWrappedStorageClient(client)
		}

		return BlobAccessInfo[*chunk.Chunk]{
			BlobAccess: referenceexpanding.NewReferenceExpandingBlobAccess(
				indirectContentAddressableStorage.BlobAccess,
				chunkBytesReader,
				chunkListFetcher,
				cdcParametersFetcher,
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
		return BlobAccessInfo[*chunk.Chunk]{}, "", status.Error(codes.InvalidArgument, "Configuration did not contain a supported storage backend")
	}
}

func (csBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[*chunk.Chunk]) blobstore.BlobAccess[*chunk.Chunk] {
	// For the Content Addressable Storage it is required that the empty
	// blob is always present. This decorator ensures that requests
	// for the empty blob never contact the storage backend.
	// More details: https://github.com/bazelbuild/bazel/issues/11063
	return blobstore.NewEmptyBlobInjectingBlobAccess(blobAccess)
}
