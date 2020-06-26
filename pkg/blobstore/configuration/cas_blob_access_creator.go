package configuration

import (
	"net/http"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/google/uuid"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type casBlobAccessCreator struct {
	casBlobReplicatorCreator

	maximumMessageSizeBytes int
}

// NewCASBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Content Addressable
// Storage.
func NewCASBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &casBlobAccessCreator{
		casBlobReplicatorCreator: casBlobReplicatorCreator{
			grpcClientFactory: grpcClientFactory,
		},
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (bac *casBlobAccessCreator) GetStorageType() blobstore.StorageType {
	return blobstore.CASStorageType
}

func (bac *casBlobAccessCreator) GetStorageTypeName() string {
	return "cas"
}

func (bac *casBlobAccessCreator) NewCustomBlobAccess(configuration *pb.BlobAccessConfiguration) (blobstore.BlobAccess, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ExistenceCaching:
		base, err := NewNestedBlobAccess(backend.ExistenceCaching.Backend, bac)
		if err != nil {
			return nil, "", err
		}
		existenceCache, err := digest.NewExistenceCacheFromConfiguration(backend.ExistenceCaching.ExistenceCache, bac.GetDigestKeyFormat(), "ExistenceCachingBlobAccess")
		if err != nil {
			return nil, "", err
		}
		return blobstore.NewExistenceCachingBlobAccess(base, existenceCache), "existence_caching", nil
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc)
		if err != nil {
			return nil, "", err
		}
		return grpcclients.NewCASBlobAccess(client, uuid.NewRandom, 65536), "grpc", nil
	case *pb.BlobAccessConfiguration_ReferenceExpanding:
		// The backend used by ReferenceExpandingBlobAccess is
		// an Indirect Content Addressable Storage (ICAS). This
		// backend stores Reference messages that point to the
		// location of a blob, not the blobs themselves. Create
		// a new BlobAccessCreator to ensure data is loaded
		// properly.
		base, err := NewNestedBlobAccess(
			backend.ReferenceExpanding,
			NewICASBlobAccessCreator(
				bac.grpcClientFactory,
				bac.maximumMessageSizeBytes))
		if err != nil {
			return nil, "", err
		}
		return blobstore.NewReferenceExpandingBlobAccess(base, http.DefaultClient, bac.maximumMessageSizeBytes), "reference_expanding", nil
	default:
		return nil, "", status.Error(codes.InvalidArgument, "Configuration did not contain a supported storage backend")
	}
}

func (bac *casBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	// For the Content Addressable Storage it is required that the empty
	// blob is always present. This decorator ensures that requests
	// for the empty blob never contact the storage backend.
	// More details: https://github.com/bazelbuild/bazel/issues/11063
	return blobstore.NewEmptyBlobInjectingBlobAccess(blobAccess)
}
