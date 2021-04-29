package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type icasBlobAccessCreator struct {
	icasBlobReplicatorCreator

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

func (bac *icasBlobAccessCreator) GetBaseDigestKeyFormat() digest.KeyFormat {
	return digest.KeyWithoutInstance
}

func (bac *icasBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.ICASReadBufferFactory
}

func (bac *icasBlobAccessCreator) GetStorageTypeName() string {
	return "icas"
}

func (bac *icasBlobAccessCreator) NewBlockListGrowthPolicy(currentBlocks, newBlocks int) (local.BlockListGrowthPolicy, error) {
	if newBlocks != 1 {
		return nil, status.Error(codes.InvalidArgument, "The number of \"new\" blocks must be set to 1 for this storage type, as objects cannot be updated reliably otherwise")
	}
	return local.NewMutableBlockListGrowthPolicy(currentBlocks), nil
}

func (bac *icasBlobAccessCreator) NewCustomBlobAccess(configuration *pb.BlobAccessConfiguration) (BlobAccessInfo, string, error) {
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
		return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Configuration did not contain a supported storage backend")
	}
}

func (bac *icasBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
