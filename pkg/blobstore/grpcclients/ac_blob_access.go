package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func getCacheCapabilities(ctx context.Context, capabilitiesClient remoteexecution.CapabilitiesClient, instanceName digest.InstanceName) (*remoteexecution.CacheCapabilities, error) {
	serverCapabilities, err := capabilitiesClient.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
		InstanceName: instanceName.String(),
	})
	if err != nil {
		return nil, err
	}
	if cacheCapabilities := serverCapabilities.CacheCapabilities; cacheCapabilities != nil {
		return cacheCapabilities, nil
	}
	return nil, status.Errorf(codes.InvalidArgument, "Instance name %#v does not support remote caching", instanceName.String())
}

type acBlobAccess struct {
	actionCacheClient       remoteexecution.ActionCacheClient
	capabilitiesClient      remoteexecution.CapabilitiesClient
	maximumMessageSizeBytes int
}

// NewACBlobAccess creates a BlobAccess handle that relays any requests
// to a GRPC service that implements the remoteexecution.ActionCache
// service. That is the service that Bazel uses to access action results
// stored in the Action Cache.
func NewACBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &acBlobAccess{
		actionCacheClient:       remoteexecution.NewActionCacheClient(client),
		capabilitiesClient:      remoteexecution.NewCapabilitiesClient(client),
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *acBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	actionResult, err := ba.actionCacheClient.GetActionResult(ctx, &remoteexecution.GetActionResultRequest{
		InstanceName: digest.GetInstanceName().String(),
		ActionDigest: digest.GetProto(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewProtoBufferFromProto(actionResult, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *acBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}
	_, err = ba.actionCacheClient.UpdateActionResult(ctx, &remoteexecution.UpdateActionResultRequest{
		InstanceName: digest.GetInstanceName().String(),
		ActionDigest: digest.GetProto(),
		ActionResult: actionResult.(*remoteexecution.ActionResult),
	})
	return err
}

func (ba *acBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "Bazel action cache does not support bulk existence checking")
}

func (ba *acBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	cacheCapabilities, err := getCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}

	// Only return fields that pertain to the Action Cache. Even though
	// 'cache_priority_capabilities' also applies to objects stored
	// in the Content Addressable Storage, it can only be set
	// through UpdateActionResult() and Execute() calls.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			ActionCacheUpdateCapabilities: cacheCapabilities.ActionCacheUpdateCapabilities,
			CachePriorityCapabilities:     cacheCapabilities.CachePriorityCapabilities,
			SymlinkAbsolutePathStrategy:   cacheCapabilities.SymlinkAbsolutePathStrategy,
		},
	}, nil
}
