package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunkmappingvalidating"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/zstd"
)

type cmsBlobAccessCreator struct {
	protoBlobAccessCreator[chunk.Mapping]
	protoBlobReplicatorCreator[chunk.Mapping]

	chunkStorage            *BlobAccessInfo[*chunk.Chunk]
	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
	zstdPool                zstd.Pool
}

// NewCMSBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for querying for chunk mapping.
func NewCMSBlobAccessCreator(chunkStorage *BlobAccessInfo[*chunk.Chunk], grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool zstd.Pool) BlobAccessCreator[chunk.Mapping] {
	return &cmsBlobAccessCreator{
		chunkStorage:            chunkStorage,
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		zstdPool:                zstdPool,
	}
}

func (cmsBlobAccessCreator) GetStorageTypeName() string {
	return "cms"
}

func (cmsBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (cmsBlobAccessCreator) GetBinaryCoder() coder.Coder[chunk.Mapping, []byte] {
	c := coder.NewChunkMappingCoder( /* prevalidated = */ true)
	return coder.JoinCoders(c, coder.NewXXH64SuffixCoder())
}

func (bac *cmsBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[chunk.Mapping]) (BlobAccessInfo[chunk.Mapping], string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ChunkMappingValidating:
		base, err := nestedCreator.NewNestedBlobAccess(backend.ChunkMappingValidating.Backend, bac)
		if err != nil {
			return BlobAccessInfo[chunk.Mapping]{}, "", err
		}
		return BlobAccessInfo[chunk.Mapping]{
			BlobAccess: chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(
				base.BlobAccess,
				bac.chunkStorage.BlobAccess,
				bac.maximumMessageSizeBytes,
				bac.zstdPool,
			),
			DigestKeyFormat: base.DigestKeyFormat.Combine(bac.chunkStorage.DigestKeyFormat),
		}, "chunk_mapping_validating", nil

	case *pb.BlobAccessConfiguration_Grpc:
		grpc := backend.Grpc
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo[chunk.Mapping]{}, "", err
		}
		ba := grpcclients.NewCMSBlobAccess(client, bac.maximumMessageSizeBytes)
		return BlobAccessInfo[chunk.Mapping]{
			BlobAccess:      ba,
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil

	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (cmsBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[chunk.Mapping]) blobstore.BlobAccess[chunk.Mapping] {
	return blobAccess
}
