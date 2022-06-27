package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

type isccBlobAccessCreator struct {
	protoBlobAccessCreator

	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
}

// NewISCCBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Initial Size Class
// Cache.
func NewISCCBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &isccBlobAccessCreator{
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (bac *isccBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.ISCCReadBufferFactory
}

func (bac *isccBlobAccessCreator) GetStorageTypeName() string {
	return "iscc"
}

func (bac *isccBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (bac *isccBlobAccessCreator) NewCustomBlobAccess(configuration *pb.BlobAccessConfiguration) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      grpcclients.NewISCCBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(bac, configuration)
	}
}

func (bac *isccBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
