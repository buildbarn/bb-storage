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
	"github.com/buildbarn/bb-storage/pkg/proto/icas"
)

type icasBlobAccessCreator struct {
	protoBlobAccessCreator[*icas.Reference]
	icasBlobReplicatorCreator

	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
}

// NewICASBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Indirect Content
// Addressable Storage.
func NewICASBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator[*icas.Reference] {
	return &icasBlobAccessCreator{
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (icasBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (icasBlobAccessCreator) GetBinaryCoder() coder.Coder[*icas.Reference, []byte] {
	c := coder.NewProtoCoder[icas.Reference]()
	return coder.JoinCoders(c, coder.NewXXH64SuffixCoder())
}

func (bac *icasBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[*icas.Reference]) (BlobAccessInfo[*icas.Reference], string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo[*icas.Reference]{}, "", err
		}
		// TODO: Should we provide a configuration option, so
		// that digest.KeyWithoutInstance can be used?
		return BlobAccessInfo[*icas.Reference]{
			BlobAccess:      grpcclients.NewICASBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (icasBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[*icas.Reference]) blobstore.BlobAccess[*icas.Reference] {
	return blobAccess
}
