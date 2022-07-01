package configuration

import (
	"context"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/completenesschecking"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/grpc"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var acCapabilitiesProvider = capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
	CacheCapabilities: &remoteexecution.CacheCapabilities{
		ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
			UpdateEnabled: true,
		},
		// CachePriorityCapabilities: Priorities not supported.
		SymlinkAbsolutePathStrategy: remoteexecution.SymlinkAbsolutePathStrategy_ALLOWED,
	},
})

type acBlobAccessCreator struct {
	protoBlobAccessCreator

	contentAddressableStorage *BlobAccessInfo
	grpcClientFactory         grpc.ClientFactory
	maximumMessageSizeBytes   int
}

// NewACBlobAccessCreator creates a BlobAccessCreator that can be
// provided to NewBlobAccessFromConfiguration() to construct a
// BlobAccess that is suitable for accessing the Action Cache.
func NewACBlobAccessCreator(contentAddressableStorage *BlobAccessInfo, grpcClientFactory grpc.ClientFactory, maximumMessageSizeBytes int) BlobAccessCreator {
	return &acBlobAccessCreator{
		contentAddressableStorage: contentAddressableStorage,
		grpcClientFactory:         grpcClientFactory,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (bac *acBlobAccessCreator) GetReadBufferFactory() blobstore.ReadBufferFactory {
	return blobstore.ACReadBufferFactory
}

func (bac *acBlobAccessCreator) GetStorageTypeName() string {
	return "ac"
}

func (bac *acBlobAccessCreator) GetDefaultCapabilitiesProvider() capabilities.Provider {
	return acCapabilitiesProvider
}

func (bac *acBlobAccessCreator) NewCustomBlobAccess(terminationContext context.Context, terminationGroup *sync.WaitGroup, configuration *pb.BlobAccessConfiguration) (BlobAccessInfo, string, error) {
	switch backend := configuration.Backend.(type) {
	case *pb.BlobAccessConfiguration_ActionResultExpiring:
		base, err := NewNestedBlobAccess(terminationContext, terminationGroup, backend.ActionResultExpiring.Backend, bac)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		minimumValidity := backend.ActionResultExpiring.MinimumValidity
		if err := minimumValidity.CheckValid(); err != nil {
			return BlobAccessInfo{}, "", util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid minimum validity")
		}
		maximumValidityJitter := backend.ActionResultExpiring.MaximumValidityJitter
		if err := maximumValidityJitter.CheckValid(); err != nil {
			return BlobAccessInfo{}, "", util.StatusWrapWithCode(err, codes.InvalidArgument, "Invalid maximum validity jitter")
		}
		return BlobAccessInfo{
			BlobAccess: blobstore.NewActionResultExpiringBlobAccess(
				base.BlobAccess,
				clock.SystemClock,
				bac.maximumMessageSizeBytes,
				minimumValidity.AsDuration(),
				maximumValidityJitter.AsDuration()),
			DigestKeyFormat: base.DigestKeyFormat,
		}, "action_result_expiring", nil
	case *pb.BlobAccessConfiguration_CompletenessChecking:
		if bac.contentAddressableStorage == nil {
			return BlobAccessInfo{}, "", status.Error(codes.InvalidArgument, "Action Cache completeness checking can only be enabled if a Content Addressable Storage is configured")
		}
		base, err := NewNestedBlobAccess(terminationContext, terminationGroup, backend.CompletenessChecking, bac)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess: completenesschecking.NewCompletenessCheckingBlobAccess(
				base.BlobAccess,
				bac.contentAddressableStorage.BlobAccess,
				blobstore.RecommendedFindMissingDigestsCount,
				bac.maximumMessageSizeBytes),
			DigestKeyFormat: base.DigestKeyFormat.Combine(bac.contentAddressableStorage.DigestKeyFormat),
		}, "completeness_checking", nil
	case *pb.BlobAccessConfiguration_Grpc:
		client, err := bac.grpcClientFactory.NewClientFromConfiguration(backend.Grpc)
		if err != nil {
			return BlobAccessInfo{}, "", err
		}
		return BlobAccessInfo{
			BlobAccess:      grpcclients.NewACBlobAccess(client, bac.maximumMessageSizeBytes),
			DigestKeyFormat: digest.KeyWithInstance,
		}, "grpc", nil
	default:
		return newProtoCustomBlobAccess(terminationContext, terminationGroup, configuration, bac)
	}
}

func (bac *acBlobAccessCreator) WrapTopLevelBlobAccess(blobAccess blobstore.BlobAccess) blobstore.BlobAccess {
	// For the Action Cache we want to ensure that all ActionResult
	// objects have a 'worker_completed_timestamp'. This is needed
	// to make decorators like ActionResultExpiringBlobAccess work.
	return blobstore.NewActionResultTimestampInjectingBlobAccess(
		blobAccess,
		clock.SystemClock,
		bac.maximumMessageSizeBytes)
}
