package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/completenesschecking"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type acBlobAccessCreator struct {
	acBlobReplicatorCreator

	contentAddressableStorage blobstore.BlobAccess
	grpcClientFactory         grpc.ClientFactory
	maximumMessageSizeBytes   int
}

// NewACBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Action Cache.
func NewACBlobAccessCreator(contentAddressableStorage blobstore.BlobAccess, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &acBlobAccessCreator{
		contentAddressableStorage: contentAddressableStorage,
		grpcClientFactory:         grpcClientFactory,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (bac *acBlobAccessCreator) GetStorageType() blobstore.StorageType {
	return blobstore.ACStorageType
}

func (bac *acBlobAccessCreator) GetStorageTypeName() string {
	return "ac"
}

func (bac *acBlobAccessCreator) NewCustomBlobAccess(configuration *pb.BlobAccessConfiguration) (blobstore.BlobAccess, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_CompletenessChecking:
		base, err := NewNestedBlobAccess(backend.CompletenessChecking, bac)
		if err != nil {
			return nil, "", err
		}
		return completenesschecking.NewCompletenessCheckingBlobAccess(
			base,
			bac.contentAddressableStorage,
			100,
			bac.maximumMessageSizeBytes), "completeness_checking", nil
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc)
		if err != nil {
			return nil, "", err
		}
		return grpcclients.NewACBlobAccess(client, bac.maximumMessageSizeBytes), "grpc", nil
	default:
		return nil, "", status.Error(codes.InvalidArgument, "Configuration did not contain a supported storage backend")
	}
}

func (bac *acBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
