package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/iscc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type isccBlobAccess struct {
	initialSizeClassCacheClient iscc.InitialSizeClassCacheClient
	maximumMessageSizeBytes     int
}

// NewISCCBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the iscc.InitialSizeClassCacheClient
// service. This is a service that is specific to Buildbarn, used to let
// the scheduler store execution times of actions, so that it can make
// better predictions about which size class to pick during future
// invocations of similar actions.
func NewISCCBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess[*iscc.PreviousExecutionStats] {
	return &isccBlobAccess{
		initialSizeClassCacheClient: iscc.NewInitialSizeClassCacheClient(client),
		maximumMessageSizeBytes:     maximumMessageSizeBytes,
	}
}

func (ba *isccBlobAccess) Get(ctx context.Context, digest digest.Digest) (*iscc.PreviousExecutionStats, error) {
	digestFunction := digest.GetDigestFunction()
	return ba.initialSizeClassCacheClient.GetPreviousExecutionStats(ctx, &iscc.GetPreviousExecutionStatsRequest{
		InstanceName:        digestFunction.GetInstanceName().String(),
		DigestFunction:      digestFunction.GetEnumValue(),
		ReducedActionDigest: digest.GetProto(),
	})
}

func (ba *isccBlobAccess) Put(ctx context.Context, digest digest.Digest, value *iscc.PreviousExecutionStats) error {
	digestFunction := digest.GetDigestFunction()
	_, err := ba.initialSizeClassCacheClient.UpdatePreviousExecutionStats(ctx, &iscc.UpdatePreviousExecutionStatsRequest{
		InstanceName:           digestFunction.GetInstanceName().String(),
		DigestFunction:         digestFunction.GetEnumValue(),
		ReducedActionDigest:    digest.GetProto(),
		PreviousExecutionStats: value,
	})
	return err
}

func (isccBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "Initial Size Class Cache does not support bulk existence checking")
}

func (isccBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
