package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
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
func NewISCCBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &isccBlobAccess{
		initialSizeClassCacheClient: iscc.NewInitialSizeClassCacheClient(client),
		maximumMessageSizeBytes:     maximumMessageSizeBytes,
	}
}

func (ba *isccBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	previousExecutionStats, err := ba.initialSizeClassCacheClient.GetPreviousExecutionStats(ctx, &iscc.GetPreviousExecutionStatsRequest{
		InstanceName:        digest.GetInstanceName().String(),
		ReducedActionDigest: digest.GetProto(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewProtoBufferFromProto(previousExecutionStats, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *isccBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *isccBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	previousExecutionStats, err := b.ToProto(&iscc.PreviousExecutionStats{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}
	_, err = ba.initialSizeClassCacheClient.UpdatePreviousExecutionStats(ctx, &iscc.UpdatePreviousExecutionStatsRequest{
		InstanceName:           digest.GetInstanceName().String(),
		ReducedActionDigest:    digest.GetProto(),
		PreviousExecutionStats: previousExecutionStats.(*iscc.PreviousExecutionStats),
	})
	return err
}

func (ba *isccBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return digest.EmptySet, status.Error(codes.Unimplemented, "Initial Size Class Cache does not support bulk existence checking")
}

func (ba *isccBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
