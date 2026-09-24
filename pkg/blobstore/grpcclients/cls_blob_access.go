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

	stream, err := ba.contentAddressableStorageClient.GetChunkMapping(ctx, &remoteexecution.GetChunkMappingRequest{
		InstanceName:     digestFunction.GetInstanceName().String(),
		BlobDigest:       blobDigest.GetProto(),
		DigestFunction:   digestFunction.GetEnumValue(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	})
	if err != nil {
		return chunk.List{}, err
	}

	// Convert wire format to chunk.List. Chunks may be spread across
	// multiple responses, so digests are collected until the server
	// closes the stream.
	chunkList := chunk.List{
		Digests: make([]digest.Digest, 0, blobstore.RecommendedFindMissingDigestsCount),
		Offsets: make([]uint64, 0, blobstore.RecommendedFindMissingDigestsCount),
	}
	offset := uint64(0)
	for {
		response, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return chunkList, nil
			}
			return chunk.List{}, err
		}
		if response.ChunkingFunction != remoteexecution.ChunkingFunction_UNKNOWN &&
			response.ChunkingFunction != remoteexecution.ChunkingFunction_REP_MAX_CDC {
			return chunk.List{}, status.Errorf(
				codes.InvalidArgument,
				"Server responded with unsupported chunking function %s",
				response.ChunkingFunction.String(),
			)
		}
		for _, proto := range response.ChunkDigests {
			d, err := digestFunction.NewDigestFromProto(proto)
			if err != nil {
				return chunk.List{}, err
			}
			chunkList.Offsets = append(chunkList.Offsets, offset)
			chunkList.Digests = append(chunkList.Digests, d)
			offset += uint64(d.GetSizeBytes())
		}
	}
}

func (ba *clsBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, value chunk.List) error {
	if len(value.Digests) == 0 {
		return status.Error(codes.InvalidArgument, "Attempted to store an empty chunk list")
	}

	digestFunction := blobDigest.GetDigestFunction()

	stream, err := ba.contentAddressableStorageClient.RegisterChunkMapping(ctx)
	if err != nil {
		return err
	}

	// Send the chunk digests in batches to stay below the maximum
	// message size. All fields other than ChunkDigests are ignored by
	// servers on subsequent requests.
	numDigests := len(value.Digests)
	i := 0
	for {
		n := blobstore.RecommendedFindMissingDigestsCount
		if n > numDigests-i {
			n = numDigests - i
		}
		chunkDigests := make([]*remoteexecution.Digest, 0, n)
		for _, digest := range value.Digests[i : i+n] {
			chunkDigests = append(chunkDigests, digest.GetProto())
		}
		request := remoteexecution.RegisterChunkMappingRequest{
			ChunkDigests: chunkDigests,
		}
		if i == 0 {
			request.InstanceName = digestFunction.GetInstanceName().String()
			request.BlobDigest = blobDigest.GetProto()
			request.DigestFunction = digestFunction.GetEnumValue()
			request.ChunkingFunction = remoteexecution.ChunkingFunction_REP_MAX_CDC
		}
		if err := stream.Send(&request); err != nil {
			return err
		}
		i += n
		if i >= numDigests {
			break
		}
	}

	_, err = stream.CloseAndRecv()
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
