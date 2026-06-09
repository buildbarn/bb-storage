package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
)

type clsBlobAccess struct {
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	maximumMessageSizeBytes         int
}

// NewCLSBlobAccess creates a BlobAccess that relays any requests to a
// gRPC server that implements the split and splice api calls of a
// remoteexecution.ContentAddressableStorage service.
func NewCLSBlobAccess(client grpc.ClientConnInterface, maximumMessageSizeBytes int) blobstore.BlobAccess {
	return &clsBlobAccess{
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		maximumMessageSizeBytes:         maximumMessageSizeBytes,
	}
}

func (ba *clsBlobAccess) Get(ctx context.Context, digest digest.Digest) buffer.Buffer {
	digestFunction := digest.GetDigestFunction()
	splitBlobsResponse, err := ba.contentAddressableStorageClient.SplitBlob(ctx, &remoteexecution.SplitBlobRequest{
		InstanceName:   digestFunction.GetInstanceName().String(),
		BlobDigest:     digest.GetProto(),
		DigestFunction: digestFunction.GetEnumValue(),
	})
	if err != nil {
		return buffer.NewBufferFromError(err)
	}
	return buffer.NewProtoBufferFromProto(splitBlobsResponse, buffer.BackendProvided(buffer.Irreparable(digest)))
}

func (ba *clsBlobAccess) GetFromComposite(ctx context.Context, parentDigest, childDigest digest.Digest, slicer slicing.BlobSlicer) buffer.Buffer {
	b, _ := slicer.Slice(ba.Get(ctx, parentDigest), childDigest)
	return b
}

func (ba *clsBlobAccess) Put(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
	splitBlobResponseProto, err := b.ToProto(&remoteexecution.SplitBlobResponse{}, ba.maximumMessageSizeBytes)
	if err != nil {
		return err
	}
	splitBlobResponse := splitBlobResponseProto.(*remoteexecution.SplitBlobResponse)
	digestFunction := digest.GetDigestFunction()
	_, err = ba.contentAddressableStorageClient.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
		InstanceName:     digestFunction.GetInstanceName().String(),
		DigestFunction:   digestFunction.GetEnumValue(),
		ChunkDigests:     splitBlobResponse.GetChunkDigests(),
		ChunkingFunction: splitBlobResponse.GetChunkingFunction(),
		BlobDigest:       digest.GetProto(),
	})
	return err
}

func (ba *clsBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Semantically an REv2 server which supports the Split and Splice
	// apis should be able to answer the SplitBlob call for any blob
	// which it has in its storage. Thus we can safely say that we are
	// able to Get a chunk list from an upstream server as long as it
	// has the blob. We can therefore reuse the existing
	// FindMissingBlobs api for this purpose.
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
			RepMaxCdcParams:   cacheCapabilities.RepMaxCdcParams,
		},
	}, nil
}
