package configuration

import (
	"sync"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type protoBlobAccessCreator struct {
	protoBlobReplicatorCreator
}

func (bac *protoBlobAccessCreator) GetBaseDigestKeyFormat() digest.KeyFormat {
	return digest.KeyWithInstance
}

func (bac *protoBlobAccessCreator) NewBlockListGrowthPolicy(currentBlocks, newBlocks int) (local.BlockListGrowthPolicy, error) {
	if newBlocks != 1 {
		return nil, status.Error(codes.InvalidArgument, "The number of \"new\" blocks must be set to 1 for this storage type, as objects cannot be updated reliably otherwise")
	}
	return local.NewMutableBlockListGrowthPolicy(currentBlocks), nil
}

func (bac *protoBlobAccessCreator) NewHierarchicalInstanceNamesLocalBlobAccess(keyLocationMap local.KeyLocationMap, locationBlobMap local.LocationBlobMap, globalLock *sync.RWMutex) (blobstore.BlobAccess, error) {
	return nil, status.Error(codes.InvalidArgument, "The hierarchical instance names option can only be used for the Content Addressable Storage")
}

func (bac *protoBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	return blobAccess
}
