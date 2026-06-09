package configuration

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklistvalidating"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type clsBlobAccessCreator struct {
	protoBlobAccessCreator
	protoBlobReplicatorCreator

	chunkStorage            *BlobAccessInfo
	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
}

// NewCLSBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for querying for chunk list.
func NewCLSBlobAccessCreator(chunkStorage *BlobAccessInfo, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &clsBlobAccessCreator{
		chunkStorage:            chunkStorage,
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (clsBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.CLSReadBufferFactory
}

func (clsBlobAccessCreator) GetStorageTypeName() string {
	return "cls"
}

func (clsBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{})
}

func (bac *clsBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ChunkListValidating:
		base, err := nestedCreator.NewNestedBlobAccess(backend.ChunkListValidating.Backend, bac)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess: chunklistvalidating.NewChunkListValidatingBlobAccess(
				base.BlobAccess,
				bac.chunkStorage.BlobAccess,
				bac.maximumMessageSizeBytes,
			),
			DigestKeyFormat: base.DigestKeyFormat.Combine(bac.chunkStorage.DigestKeyFormat),
		}, "chunk_list_validating", nil

	case *pb.BlobAccessConfiguration_Grpc:
		grpc := backend.Grpc
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		ba := grpcclients.NewCLSBlobAccess(client, bac.maximumMessageSizeBytes)
		if grpc.CapabilitiesCache != nil {
			cache, err := blobstore.NewTTLCacheFromConfiguration[*remoteexecution.ServerCapabilities](
				grpc.CapabilitiesCache,
				clock.SystemClock,
				"CLSCapabilityCachingBlobStore",
			)
			if err != nil {
				return BlobAccessInfo{}, "", util.StatusWrap(err, "Failed to create capabilities cache")
			}
			ba = blobstore.NewCapabilitiesCachingBlobAccess(ba, cache)
		}
		return BlobAccessInfo{
			BlobAccess:      ba,
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil

	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (clsBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
