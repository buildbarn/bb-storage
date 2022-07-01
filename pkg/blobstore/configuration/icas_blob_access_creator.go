package configuration

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

type icasBlobAccessCreator struct {
	protoBlobAccessCreator

	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
}

// NewICASBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Indirect Content
// Addressable Storage.
func NewICASBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &icasBlobAccessCreator{
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (bac *icasBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.ICASReadBufferFactory
}

func (bac *icasBlobAccessCreator) GetStorageTypeName() string {
	return "icas"
}

func (bac *icasBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (bac *icasBlobAccessCreator) NewCustomBlobAccess(terminationContext context.Context, terminationGroup *sync.WaitGroup, configuration *pb.BlobAccessConfiguration) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		// TODO: Should we provide a configuration option, so
		// that digest.KeyWithoutInstance can be used?
		return BlobAccessInfo{
			BlobAccess:      grpcclients.NewICASBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(terminationContext, terminationGroup, configuration, bac)
	}
}

func (bac *icasBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
