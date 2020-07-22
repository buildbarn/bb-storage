package replication

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
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
	b := br.source.Get(ctx, digest)
	b, t := buffer.WithBackgroundTask(b)
	go func() {
		// Let the remote replication service perform the
		// replication while we stream data back to the client.
		_, err := br.replicatorClient.ReplicateBlobs(ctx, &replicator.ReplicateBlobsRequest{
			InstanceName: digest.GetInstanceName().String(),
			BlobDigests: []*remoteexecution.Digest{
				digest.GetProto(),
			},
		})
		t.Finish(err)
	}()
	return b
}

func (br *remoteBlobReplicator) ReplicateMultiple(ctx context.Context, digests digest.Set) error {
	// Partition all digests by instance name, as the
	// ReplicateBlobs() RPC can only process digests for a single
	// instance. This is not a serious limitation, as digest sets
	// are unlikely to contain digests for multiple instance names.
	perInstanceDigests := map[digest.InstanceName][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		instanceName := digest.GetInstanceName()
		perInstanceDigests[instanceName] = append(perInstanceDigests[instanceName], digest.GetProto())
	}
	for instanceName, blobDigests := range perInstanceDigests {
		// Call ReplicateBlobs() for each instance.
		request := replicator.ReplicateBlobsRequest{
			InstanceName: instanceName.String(),
			BlobDigests:  blobDigests,
		}
		if _, err := br.replicatorClient.ReplicateBlobs(ctx, &request); err != nil {
			return err
		}
	}
	return nil
}
