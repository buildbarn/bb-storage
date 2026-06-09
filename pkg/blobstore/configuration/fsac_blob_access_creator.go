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
	"github.com/buildbarn/bb-storage/pkg/proto/fsac"
	"github.com/buildbarn/bb-storage/pkg/zstd"
)

type fsacBlobAccessCreator struct {
	protoBlobAccessCreator[*fsac.FileSystemAccessProfile]
	protoBlobReplicatorCreator[*fsac.FileSystemAccessProfile]

	grpcClientFactory       grpc.ClientFactory
	maximumMessageSizeBytes int
	zstdPool                zstd.Pool
}

// NewFSACBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the File System Access
// Cache.
func NewFSACBlobAccessCreator(grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int, zstdPool zstd.Pool) BlobAccessCreator[*fsac.FileSystemAccessProfile] {
	return &fsacBlobAccessCreator{
		grpcClientFactory:       grpcClientFactory,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		zstdPool:                zstdPool,
	}
}

func (fsacBlobAccessCreator) GetStorageTypeName() string {
	return "fsac"
}

func (fsacBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return nil
}

func (bac *fsacBlobAccessCreator) GetBinaryCoder() coder.Coder[*fsac.FileSystemAccessProfile, []byte] {
	c := coder.NewProtoCoder[fsac.FileSystemAccessProfile]()
	c = coder.JoinCoders(c, coder.NewZSTDCoder(bac.zstdPool))
	return coder.JoinCoders(c, coder.NewXXH64SuffixCoder())
}

func (bac *fsacBlobAccessCreator) NewCustomBlobAccess(terminationGroup program.Group, configuration *pb.BlobAccessConfiguration, nestedCreator NestedBlobAccessCreator[*fsac.FileSystemAccessProfile]) (BlobAccessInfo[*fsac.FileSystemAccessProfile], string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc.Client, terminationGroup)
		if err != nil {
			return BlobAccessInfo[*fsac.FileSystemAccessProfile]{}, "", err
		}
		return BlobAccessInfo[*fsac.FileSystemAccessProfile]{
			BlobAccess:      grpcclients.NewFSACBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(configuration, nestedCreator, bac)
	}
}

func (fsacBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess[*fsac.FileSystemAccessProfile]) blobstore.BlobAccess[*fsac.FileSystemAccessProfile] {
	return blobAccess
}
