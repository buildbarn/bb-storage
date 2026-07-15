package grpcclients

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/slicing"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	missing := digest.NewSetBuilder(digests.Length())
	for _, d := range digests.Items() {
		_, err := ba.contentAddressableStorageClient.SplitBlob(ctx, &remoteexecution.SplitBlobRequest{
			InstanceName:     d.GetInstanceName().String(),
			BlobDigest:       d.GetProto(),
			DigestFunction:   d.GetDigestFunction().GetEnumValue(),
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		})
		if status.Code(err) == codes.NotFound {
			missing.Add(d)
		} else if err != nil {
			return digest.EmptySet, err
		}
	}
	return missing.Build(), nil
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
