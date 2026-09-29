package grpcclients

import (
	"bytes"
	"context"
	"io"
	"slices"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type csBlobAccess struct {
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	supportedCompressors            atomic.Pointer[[]remoteexecution.Compressor_Value]
	zstdPool                        bb_zstd.Pool
	preferCompression               bool
}

// NewCSBlobAccess creates a BlobAccess handle that relays any requests
// to a gRPC service that implements the
// remoteexecution.ContentAddressableStorage services. Those are the
// services that Bazel uses to access blobs stored in the Content
// Addressable Storage.
//
// If preferCompression is true, the client will use ZSTD compression
// for operations if the server supports it.
func NewCSBlobAccess(client grpc.ClientConnInterface, zstdPool bb_zstd.Pool, preferCompression bool) blobstore.BlobAccess[*chunk.Chunk] {
	return &csBlobAccess{
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		zstdPool:                        zstdPool,
		preferCompression:               preferCompression,
	}
}

// shouldUseZSTDCompression checks if ZSTD compression should be used.
// It ensures GetCapabilities has been called to negotiate compression support.
func (ba *csBlobAccess) shouldUseZSTDCompression(ctx context.Context, digest digest.Digest) (bool, error) {
	if !ba.preferCompression {
		return false, nil
	}

	supportedCompressors := ba.supportedCompressors.Load()
	if supportedCompressors == nil {
		// Call GetCapabilities to check server support. GetCapabilities
		// will populate the supported compressors atomic.
		if _, err := ba.GetCapabilities(ctx, digest.GetDigestFunction().GetInstanceName()); err != nil {
			return false, err
		}
		supportedCompressors = ba.supportedCompressors.Load()
	}

	return slices.Contains(*supportedCompressors, remoteexecution.Compressor_ZSTD), nil
}

func (ba *csBlobAccess) Get(ctx context.Context, digest digest.Digest) (*chunk.Chunk, error) {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		return nil, err
	}

	compressor := remoteexecution.Compressor_IDENTITY
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
	}

	digestFunction := digest.GetDigestFunction()
	req := &remoteexecution.BatchReadBlobsRequest{
		InstanceName:          digest.GetInstanceName().String(),
		Digests:               []*remoteexecution.Digest{digest.GetProto()},
		AcceptableCompressors: []remoteexecution.Compressor_Value{compressor},
		DigestFunction:        digestFunction.GetEnumValue(),
	}

	resp, err := ba.contentAddressableStorageClient.BatchReadBlobs(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(resp.GetResponses()) != 1 {
		return nil, status.Errorf(codes.Internal, "Expected 1 response, got %d", len(resp.Responses))
	}

	r := resp.Responses[0]
	if err := status.ErrorProto(r.Status); err != nil {
		return nil, err
	}

	generator := digestFunction.NewGenerator(digest.GetSizeBytes())
	var data, compressedData []byte
	switch r.Compressor {
	case remoteexecution.Compressor_IDENTITY:
		data = r.Data
	case remoteexecution.Compressor_ZSTD:
		decoder, err := ba.zstdPool.NewDecoder(ctx, bytes.NewReader(r.Data))
		if err != nil {
			return nil, err
		}
		defer decoder.Close()
		data = make([]byte, digest.GetSizeBytes())
		if _, err := io.ReadFull(decoder, data); err != nil {
			return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to decompress blob")
		}
		// The compressed representation is stored as-is, so the stream
		// may not decompress to more than the advertised size.
		var eofBuf [1]byte
		if n, err := decoder.Read(eofBuf[:]); n > 0 || err != io.EOF {
			return nil, status.Error(codes.Internal, "Decompressed stream yielded more data than expected")
		}
		compressedData = r.Data
	default:
		return nil, status.Errorf(codes.Internal, "Unsupported upstream compresssion algorithm %s", r.Compressor.String())
	}

	if _, err := generator.Write(data); err != nil {
		return nil, err
	}
	if actualDigest := generator.Sum(); actualDigest != digest {
		return nil, status.Errorf(codes.Internal, "Digest mismatch, expected %s, got %s", digest.String(), actualDigest.String())
	}

	if compressedData == nil {
		return chunk.NewChunk(ba.zstdPool, data), nil
	}
	return chunk.NewChunkWithCompressedData(data, compressedData), nil
}

func (ba *csBlobAccess) Put(ctx context.Context, digest digest.Digest, value *chunk.Chunk) error {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		return err
	}

	var compressor remoteexecution.Compressor_Value
	var data []byte
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
		data, err = value.GetBytesCompressed(ctx)
		if err != nil {
			return err
		}
	} else {
		compressor = remoteexecution.Compressor_IDENTITY
		data = value.GetBytes()
	}

	req := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName:   digest.GetInstanceName().String(),
		DigestFunction: digest.GetDigestFunction().GetEnumValue(),
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{Digest: digest.GetProto(), Data: data, Compressor: compressor},
		},
	}

	resp, err := ba.contentAddressableStorageClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Responses) != 1 {
		return status.Errorf(codes.Internal, "Expected 1 response, got %d", len(resp.Responses))
	}
	r := resp.Responses[0]
	if r.GetStatus().GetCode() != int32(codes.OK) {
		return status.ErrorProto(r.Status)
	}
	return nil
}

func (ba *csBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return findMissingBlobsInternal(ctx, digests, ba.contentAddressableStorageClient)
}

func findMissingBlobsInternal(ctx context.Context, digests digest.Set, cas remoteexecution.ContentAddressableStorageClient) (digest.Set, error) {
	// Partition all digests by digest function, as the
	// FindMissingBlobs() RPC can only process digests for a single
	// instance name and digest function.
	perFunctionDigests := map[digest.Function][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		digestFunction := digest.GetDigestFunction()
		perFunctionDigests[digestFunction] = append(perFunctionDigests[digestFunction], digest.GetProto())
	}

	missingDigests := digest.NewSetBuilder(0)
	for digestFunction, blobDigests := range perFunctionDigests {
		// Call FindMissingBlobs() for each digest function.
		request := remoteexecution.FindMissingBlobsRequest{
			InstanceName:   digestFunction.GetInstanceName().String(),
			BlobDigests:    blobDigests,
			DigestFunction: digestFunction.GetEnumValue(),
		}
		response, err := cas.FindMissingBlobs(ctx, &request)
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

func (ba *csBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := getServerCapabilitiesWithCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}

	cacheCapabilities := serverCapabilities.CacheCapabilities

	// Store supported compressors for compression negotiation.
	ba.supportedCompressors.Store(&cacheCapabilities.SupportedCompressors)

	if !cacheCapabilities.SplitBlobSupport {
		return nil, status.Errorf(codes.Internal, "Upstream server does not support split blob requests")
	}
	if !cacheCapabilities.SpliceBlobSupport {
		return nil, status.Errorf(codes.Internal, "Upstream server does not support splice blob requests")
	}
	repMaxCdcParams := cacheCapabilities.RepMaxCdcParams
	if repMaxCdcParams == nil {
		return nil, status.Errorf(codes.Internal, "Upstream server does not support RepMaxCDC")
	}
	if repMaxCdcParams.MinChunkSizeBytes < 64 {
		return nil, status.Errorf(codes.Internal, "Upstream server advertises a RepMaxCDC minimum chunk size of %d bytes, but a minimum of 64 bytes is required", repMaxCdcParams.MinChunkSizeBytes)
	}

	// Only return fields that pertain to the Content Addressable
	// Storage. Don't set 'max_batch_total_size_bytes', as we don't
	// issue batch operations.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: digest.RemoveUnsupportedDigestFunctions(cacheCapabilities.DigestFunctions),
			RepMaxCdcParams: cacheCapabilities.RepMaxCdcParams,
		},
		DeprecatedApiVersion: serverCapabilities.DeprecatedApiVersion,
		LowApiVersion:        serverCapabilities.LowApiVersion,
		HighApiVersion:       serverCapabilities.HighApiVersion,
	}, nil
}
