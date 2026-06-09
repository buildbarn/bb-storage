package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklistvalidating"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/zstd"
)

type clsBlobAccessCreator struct {
	protoBlobAccessCreator[chunklist.ChunkList]
	protoBlobReplicatorCreator[chunklist.ChunkList]

	chunkStorage            *BlobAccessInfo[*buffer.Chunk]
	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
	zstdPool                zstd.Pool
}

// NewCLSBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for querying for chunk list.
func NewCLSBlobAccessCreator(chunkStorage *BlobAccessInfo[*buffer.Chunk], grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool zstd.Pool) BlobAccessCreator[chunklist.ChunkList] {
	return &clsBlobAccessCreator{
		chunkStorage:            chunkStorage,
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		zstdPool:                zstdPool,
	}
}

func (clsBlobAccessCreator) GetStorageTypeName() string {
	return "cls"
}

func (clsBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (bac *clsBlobAccessCreator) GetBinaryCoder() coder.Coder[chunklist.ChunkList, []byte] {
	c := coder.NewChunkListCoder( /* prevalidated = */ true)
	c = coder.JoinCoders(c, coder.NewZSTDCoder(bac.zstdPool))
	return coder.JoinCoders(c, coder.NewXXH64SuffixCoder())
}

func (bac *clsBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[chunklist.ChunkList]) (BlobAccessInfo[chunklist.ChunkList], string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ChunkListValidating:
		base, err := nestedCreator.NewNestedBlobAccess(backend.ChunkListValidating.Backend, bac)
		if err != nil {
			return BlobAccessInfo[chunklist.ChunkList]{}, "", err
		}
		return BlobAccessInfo[chunklist.ChunkList]{
			BlobAccess: chunklistvalidating.NewChunkListValidatingBlobAccess(
				base.BlobAccess,
				bac.chunkStorage.BlobAccess,
				bac.maximumMessageSizeBytes,
				bac.zstdPool,
			),
			DigestKeyFormat: base.DigestKeyFormat.Combine(bac.chunkStorage.DigestKeyFormat),
		}, "chunk_list_validating", nil

	case *pb.BlobAccessConfiguration_Grpc:
		grpc := backend.Grpc
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo[chunklist.ChunkList]{}, "", err
		}
		ba := grpcclients.NewCLSBlobAccess(client, bac.maximumMessageSizeBytes)
		return BlobAccessInfo[chunklist.ChunkList]{
			BlobAccess:      ba,
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil

	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (clsBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[chunklist.ChunkList]) blobstore.BlobAccess[chunklist.ChunkList] {
	return blobAccess
}
