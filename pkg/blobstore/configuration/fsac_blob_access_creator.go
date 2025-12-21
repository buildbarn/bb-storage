package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
)

type fsacBlobAccessCreator struct {
	protoBlobAccessCreator
	protoBlobReplicatorCreator

	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
}

// NewFSACBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the File System Access
// Cache.
func NewFSACBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &fsacBlobAccessCreator{
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (fsacBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.FSACReadBufferFactory
}

func (fsacBlobAccessCreator) GetStorageTypeName() string {
	return "fsac"
}

func (fsacBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (bac *fsacBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      grpcclients.NewFSACBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (fsacBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
