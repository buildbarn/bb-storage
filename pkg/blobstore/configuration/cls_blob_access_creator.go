package configuration

import (
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklistvalidating"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clsBlobAccessCreator struct {
	protoBlobAccessCreator
	protoBlobReplicatorCreator

	contentAddressableStorage *BlobAccessInfo
	grpcClientFactory         grpc.ClientFactory
	maximumMessageSizeBytes   int
}

// NewCLSBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for querying for chunk list.
func NewCLSBlobAccessCreator(contentAddressableStorage *BlobAccessInfo, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &clsBlobAccessCreator{
		contentAddressableStorage: contentAddressableStorage,
		grpcClientFactory:         grpcClientFactory,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
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
		if bac.contentAddressableStorage == nil {
			return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Chunk list validation can only be enabled if a Content Addressable Storage is configured")
		}

		base, err := nestedCreator.NewNestedBlobAccess(backend.ChunkListValidating.Backend, bac)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess: chunklistvalidating.NewChunkListValidatingBlobAccess(
				base.BlobAccess,
				bac.contentAddressableStorage.BlobAccess,
				bac.maximumMessageSizeBytes,
			),
			DigestKeyFormat: base.DigestKeyFormat.Combine(bac.contentAddressableStorage.DigestKeyFormat),
		}, "chunk_list_validating", nil

	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      grpcclients.NewCLSBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil

	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (clsBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
