package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/coder"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"
	"github.com/buildbarn/bb-storage/pkg/zstd"
)

type isccBlobAccessCreator struct {
	protoBlobAccessCreator[*iscc.PreviousExecutionStats]
	protoBlobReplicatorCreator[*iscc.PreviousExecutionStats]

	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
	zstdPool                zstd.Pool
}

// NewISCCBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Initial Size Class
// Cache.
func NewISCCBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool zstd.Pool) BlobAccessCreator[*iscc.PreviousExecutionStats] {
	return &isccBlobAccessCreator{
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		zstdPool:                zstdPool,
	}
}

func (isccBlobAccessCreator) GetStorageTypeName() string {
	return "iscc"
}

func (isccBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (bac *isccBlobAccessCreator) GetBinaryCoder() coder.Coder[*iscc.PreviousExecutionStats, []byte] {
	c := coder.NewProtoCoder[iscc.PreviousExecutionStats]()
	c = coder.JoinCoders(c, coder.NewZSTDCoder(bac.zstdPool))
	return coder.JoinCoders(c, coder.NewXXH64SuffixCoder())
}

func (bac *isccBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[*iscc.PreviousExecutionStats]) (BlobAccessInfo[*iscc.PreviousExecutionStats], string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo[*iscc.PreviousExecutionStats]{}, "", err
		}
		return BlobAccessInfo[*iscc.PreviousExecutionStats]{
			BlobAccess:      grpcclients.NewISCCBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (isccBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[*iscc.PreviousExecutionStats]) blobstore.BlobAccess[*iscc.PreviousExecutionStats] {
	return blobAccess
}
