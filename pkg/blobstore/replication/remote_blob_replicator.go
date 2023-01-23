package replication

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/replicator"

	"google.golang.org/grpc"
)

type remoteBlobReplicator struct {
	source           blobstore.BlobAccess
	replicatorClient replicator.ReplicatorClient
}

// NewRemoteBlobReplicator creates a BlobReplicator that forwards
// requests to a remote gRPC service. This service may be used to
// deduplicate and queue replication actions globally.
func NewRemoteBlobReplicator(source blobstore.BlobAccess, client grpc.ClientConnInterface) BlobReplicator {
	return &remoteBlobReplicator{
		source:           source,
		replicatorClient: replicator.NewReplicatorClient(client),
	}
}

func (br *remoteBlobReplicator) ReplicateSingle(ctx context.Context, digest digest.Digest) buffer.Buffer {
	return br.source.Get(ctx, digest).WithTask(func() error {
		// Let the remote replication service perform the
		// replication while we stream data back to the client.
		digestFunction := digest.GetDigestFunction()
		_, err := br.replicatorClient.ReplicateBlobs(ctx, &replicator.ReplicateBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			DigestFunction: digestFunction.GetEnumValue(),
			BlobDigests: []*remoteexecution.Digest{
				digest.GetProto(),
			},
		})
		return err
	})
}

func (br *remoteBlobReplicator) ReplicateComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	return br.source.GetFromComposite(ctx, parentDigest, childDigest, slicer).WithTask(func() error {
		digestFunction := parentDigest.GetDigestFunction()
		_, err := br.replicatorClient.ReplicateBlobs(ctx, &replicator.ReplicateBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			DigestFunction: digestFunction.GetEnumValue(),
			BlobDigests: []*remoteexecution.Digest{
				parentDigest.GetProto(),
			},
		})
		return err
	})
}

func (br *remoteBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	// Partition all digests by digest function, as the
	// ReplicateBlobs() RPC can only process digests for a single
	// instance name and digest function. This is not a serious
	// limitation, as digest sets are unlikely to contain digests
	// for multiple instance names.
	perFunctionDigests := map[digest.Function][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		digestFunction := digest.GetDigestFunction()
		perFunctionDigests[digestFunction] = append(perFunctionDigests[digestFunction], digest.GetProto())
	}
	for digestFunction, blobDigests := range perFunctionDigests {
		// Call ReplicateBlobs() for each digest function.
		request := replicator.ReplicateBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			DigestFunction: digestFunction.GetEnumValue(),
			BlobDigests:    blobDigests,
		}
		if _, err := br.replicatorClient.ReplicateBlobs(ctx, &request); err != nil {
			return err
		}
	}
	return nil
}
