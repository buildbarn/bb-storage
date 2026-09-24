package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
)

type clsBlobAccess struct {
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	maximumMessageSizeBytes         int
}

// NewCLSBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the split and splice API calls of a
// remoteexecution.ContentAddressableStorage service.
func NewCLSBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess[chunk.List] {
	return &clsBlobAccess{
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		maximumMessageSizeBytes:         maximumMessageSizeBytes,
	}
}

func (ba *clsBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) (chunk.List, error) {
	digestFunction := blobDigest.GetDigestFunction()

	// TODO: Replace with streaming variant
	splitBlobsResponse, err := ba.contentAddressableStorageClient.SplitBlob(ctx, &remoteexecution.SplitBlobRequest{
		InstanceName:   digestFunction.GetInstanceName().String(),
		BlobDigest:     blobDigest.GetProto(),
		DigestFunction: digestFunction.GetEnumValue(),
	})
	if err != nil {
		return chunk.List{}, err
	}

	// Convert wire format to chunk.List
	chunkList := chunk.List{
		Offsets: make([]uint64, len(splitBlobsResponse.ChunkDigests)),
		Digests: make([]digest.Digest, len(splitBlobsResponse.ChunkDigests)),
	}
	offset := uint64(0)
	for i, proto := range splitBlobsResponse.ChunkDigests {
		d, err := digestFunction.NewDigestFromProto(proto)
		if err != nil {
			return chunk.List{}, err
		}
		chunkList.Offsets[i] = offset
		chunkList.Digests[i] = d
		offset += uint64(d.GetSizeBytes())
	}
	return chunkList, nil
}

func (ba *clsBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, value chunk.List) error {
	// Convert chunk.List to wire format
	chunkDigests := make([]*remoteexecution.Digest, 0, len(value.Digests))
	for _, digest := range value.Digests {
		chunkDigests = append(chunkDigests, digest.GetProto())
	}

	digestFunction := blobDigest.GetDigestFunction()
	// TODO: Replace with streaming variant
	_, err := ba.contentAddressableStorageClient.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
		InstanceName:     digestFunction.GetInstanceName().String(),
		DigestFunction:   digestFunction.GetEnumValue(),
		ChunkDigests:     chunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		BlobDigest:       blobDigest.GetProto(),
	})
	return err
}

func (ba *clsBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Semantically an REv2 server which supports the Split and Splice
	// apis should be able to answer the SplitBlob call for any blob
	// which it has in its storage. Thus we can safely say that we are
	// able to Get a chunk list from an upstream server as long as it
	// has the blob. We can therefore reuse the existing
	// FindMissingBlobs API for this purpose.
	//
	// In Buildbarn we implement this on the server side by segregating
	// FMB requests for blobs larger than the maximum chunk size to the
	// Chunk List Storage (CLS) and to the Chunk Storage (CS) for other
	// blobs.
	return findMissingBlobsInternal(ctx, digests, ba.contentAddressableStorageClient)
}

func (clsBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
