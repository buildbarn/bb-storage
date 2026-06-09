package grpcclients

import (
	"context"
	"io"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cmsBlobAccess struct {
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	maximumMessageSizeBytes         int
}

// NewCMSBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the split and splice API calls of a
// remoteexecution.ContentAddressableStorage service.
func NewCMSBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess[chunk.Mapping] {
	return &cmsBlobAccess{
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		maximumMessageSizeBytes:         maximumMessageSizeBytes,
	}
}

func (ba *cmsBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) (chunk.Mapping, error) {
	digestFunction := blobDigest.GetDigestFunction()

	stream, err := ba.contentAddressableStorageClient.GetChunkMapping(ctx, &remoteexecution.GetChunkMappingRequest{
		InstanceName:     digestFunction.GetInstanceName().String(),
		BlobDigest:       blobDigest.GetProto(),
		DigestFunction:   digestFunction.GetEnumValue(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	})
	if err != nil {
		return chunk.Mapping{}, err
	}

	// Convert wire format to chunk.Mapping. Chunks may be spread across
	// multiple responses, so digests are collected until the server
	// closes the stream.
	var protoDigests []*remoteexecution.Digest
	response, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return chunk.Mapping{}, status.Error(codes.Internal, "Server closed connection before first message")
		}
		return chunk.Mapping{}, err
	}
	if response.ChunkingFunction != remoteexecution.ChunkingFunction_REP_MAX_CDC {
		return chunk.Mapping{}, status.Errorf(
			codes.Internal,
			"Server responded with unsupported chunking function %s",
			response.ChunkingFunction.String(),
		)
	}
	for {
		protoDigests = append(protoDigests, response.ChunkDigests...)
		response, err = stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return chunk.Mapping{}, err
		}
	}
	return chunk.NewMappingFromProtoDigests(digestFunction, protoDigests, uint64(blobDigest.GetSizeBytes()), false)
}

func (ba *cmsBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, value chunk.Mapping) error {
	digestFunction := blobDigest.GetDigestFunction()

	stream, err := ba.contentAddressableStorageClient.RegisterChunkMapping(ctx)
	if err != nil {
		return err
	}

	// Send the chunk digests in batches to stay below the maximum
	// message size. All fields other than ChunkDigests are ignored by
	// servers on subsequent requests. At least one request is always
	// sent, so that the server can identify the blob being registered
	// even for empty chunk mappings.
	protoDigests := value.GetProtoDigests()
	n := min(len(protoDigests), blobstore.RecommendedFindMissingDigestsCount)
	if err := stream.Send(&remoteexecution.RegisterChunkMappingRequest{
		ChunkDigests:     protoDigests[:n],
		InstanceName:     digestFunction.GetInstanceName().String(),
		BlobDigest:       blobDigest.GetProto(),
		DigestFunction:   digestFunction.GetEnumValue(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}); err != nil {
		return err
	}
	protoDigests = protoDigests[n:]
	for len(protoDigests) > 0 {
		n = min(len(protoDigests), blobstore.RecommendedFindMissingDigestsCount)
		if err := stream.Send(&remoteexecution.RegisterChunkMappingRequest{
			ChunkDigests: protoDigests[:n],
		}); err != nil {
			return err
		}
	}

	_, err = stream.CloseAndRecv()
	return err
}

func (ba *cmsBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Semantically an REv2 server which supports the Split and Splice
	// apis should be able to answer the SplitBlob call for any blob
	// which it has in its storage. Thus we can safely say that we are
	// able to Get a chunk mapping from an upstream server as long as it
	// has the blob. We can therefore reuse the existing
	// FindMissingBlobs API for this purpose.
	//
	// In Buildbarn we implement this on the server side by segregating
	// FMB requests for blobs larger than the maximum chunk size to the
	// Chunk Mapping Storage (CMS) and to the Chunk Storage (CS) for other
	// blobs.
	return findMissingBlobsInternal(ctx, digests, ba.contentAddressableStorageClient)
}

func (cmsBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	panic("GetCapabilities() should only be called against BlobAccess instances for the Content Addressable Storage and Action Cache")
}
