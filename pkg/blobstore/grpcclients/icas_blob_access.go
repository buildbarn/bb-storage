package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/proto/icas"

	"google.golang.org/grpc"
)

type icasBlobAccess struct {
	icasClient              icas.IndirectContentAddressableStorageClient
	maximumMessageSizeBytes int
}

// NewICASBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the icas.IndirectContentAddressableStorage
// service. This is a service that is specific to Buildbarn, used to
// track references to objects stored in external corpora.
func NewICASBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &icasBlobAccess{
		icasClient:              icas.NewIndirectContentAddressableStorageClient(client),
		maximumMessageSizeBytes: maximumMessageSizeBytes,
	}
}

func (ba *icasBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	reference, err := ba.icasClient.GetReference(ctx, &icas.GetReferenceRequest{
		InstanceName: digest.GetInstanceName().String(),
		Digest:       digest.GetProto(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewProtoBufferFromProto(reference, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *icasBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *icasBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	reference, err := b.ToProto(&icas.Reference{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}
	// TODO: The ICAS protocol allows us to do batch updates, while
	// BlobAccess has no mechanics for that. We should extend
	// BlobAccess to support that.
	digestFunction := digest.GetDigestFunction()
	_, err = ba.icasClient.BatchUpdateReferences(ctx, &icas.BatchUpdateReferencesRequest{
		InstanceName: digestFunction.GetInstanceName().String(),
		Requests: []*icas.BatchUpdateReferencesRequest_Request{
			{
				Digest:    digest.GetProto(),
				Reference: reference.(*icas.Reference),
			},
		},
		DigestFunction: digestFunction.GetEnumValue(),
	})
	return err
}

func (ba *icasBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition all digests by instance name, as the
	// FindMissingReferences() RPC can only process digests for a
	// single instance.
	perInstanceDigests := map[digest.Function][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		digestFunction := digest.GetDigestFunction()
		perInstanceDigests[digestFunction] = append(perInstanceDigests[digestFunction], digest.GetProto())
	}

	missingDigests := digest.NewSetBuilder()
	for digestFunction, blobDigests := range perInstanceDigests {
		// Call FindMissingReferences() for each instance.
		request := remoteexecution.FindMissingBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			BlobDigests:    blobDigests,
			DigestFunction: digestFunction.GetEnumValue(),
		}
		response, err := ba.icasClient.FindMissingReferences(ctx, &request)
		if err != nil {
			return digest.EmptySet, err
		}

		// Convert results back.
		for _, proto := range response.MissingBlobDigests {
			blobDigest, err := digestFunction.NewDigestFromProto(proto)
			if err != nil {
				return digest.EmptySet, err
			}
			missingDigests.Add(blobDigest)
		}
	}
	return missingDigests.Build(), nil
}

func (ba *icasBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
