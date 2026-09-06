package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"
	chunklist_pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/chunklist"

	"google.golang.org/grpc"
)

type clsBlobAccess struct {
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	maximumMessageSizeBytes         int
}

// NewCLSBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the split and splice API calls of a
// remoteexecution.ContentAddressableStorage service.
func NewCLSBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &clsBlobAccess{
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		maximumMessageSizeBytes:         maximumMessageSizeBytes,
	}
}

func (ba *clsBlobAccess) Get(ctx context.Context, blobDigest digest.Digest) buffer.Buffer {
	digestFunction := blobDigest.GetDigestFunction()
	splitBlobsResponse, err := ba.contentAddressableStorageClient.SplitBlob(ctx, &remoteexecution.SplitBlobRequest{
		InstanceName:   digestFunction.GetInstanceName().String(),
		BlobDigest:     blobDigest.GetProto(),
		DigestFunction: digestFunction.GetEnumValue(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}

	// Convert wire format to storage format.
	chunkDigests := make([]digest.Digest, 0, len(splitBlobsResponse.ChunkDigests))
	for _, proto := range splitBlobsResponse.ChunkDigests {
		d, err := digestFunction.NewDigestFromProto(proto)
		if err != nil {
			return buffer.NewBufferFromError(err)
		}
		chunkDigests = append(chunkDigests, d)
	}
	return buffer.NewProtoBufferFromProto(
		&chunklist_pb.ChunkList{
			Data: chunklist.EncodeToBinary(chunkDigests),
		},
		buffer.BackendProvided(buffer.Irreparable(blobDigest)),
	)
}

func (ba *clsBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *clsBlobAccess) Put(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
	msg, err := b.ToProto(&chunklist_pb.ChunkList{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}

	// Convert storage format to wire format.
	chunkList, err := chunklist.NewChunkListFromProto(msg.(*chunklist_pb.ChunkList), blobDigest.GetInstanceName())
	if err != nil {
		return err
	}
	chunkDigests := make([]*remoteexecution.Digest, 0, len(chunkList))
	for _, entry := range chunkList {
		chunkDigests = append(chunkDigests, entry.Digest.GetProto())
	}

	digestFunction := blobDigest.GetDigestFunction()
	_, err = ba.contentAddressableStorageClient.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
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

func (ba *clsBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := getServerCapabilitiesWithCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}
	cacheCapabilities := serverCapabilities.CacheCapabilities
	// Only return fields that pertain to Chunk List Storage.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			SplitBlobSupport:  cacheCapabilities.SplitBlobSupport,
			SpliceBlobSupport: cacheCapabilities.SpliceBlobSupport,
		},
	}, nil
}
