package grpcservers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type contentAddressableStorageServer struct {
	chunkStorage              blobstore.BlobAccess[*chunk.Chunk]
	chunkMappingStorage       blobstore.BlobAccess[chunk.Mapping]
	chunkMappingFetcher       chunk.MappingFetcher
	cdcParametersFetcher      capabilities.CDCParametersFetcher
	zstdPool                  zstd.Pool
	readerPutter              cas.ReaderPutter
	getChunkMappingAuthorizer auth.Authorizer
	putChunkMappingAuthorizer auth.Authorizer
	maximumMessageSizeBytes   int64
	maximumChunkCount         int
}

// NewContentAddressableStorageServer creates a GRPC service for serving
// the contents of a Bazel Content Addressable Storage (CAS) to Bazel.
func NewContentAddressableStorageServer(chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], cdcParametersFetcher capabilities.CDCParametersFetcher, zstdPool zstd.Pool, readerPutter cas.ReaderPutter, getChunkMappingAuthorizer, putChunkMappingAuthorizer auth.Authorizer, maximumMessageSizeBytes int64, maximumChunkCount int) remoteexecution.ContentAddressableStorageServer {
	return &contentAddressableStorageServer{
		chunkStorage:              chunkStorage,
		chunkMappingStorage:       chunkMappingStorage,
		cdcParametersFetcher:      cdcParametersFetcher,
		zstdPool:                  zstdPool,
		readerPutter:              readerPutter,
		getChunkMappingAuthorizer: getChunkMappingAuthorizer,
		putChunkMappingAuthorizer: putChunkMappingAuthorizer,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		maximumChunkCount:         maximumChunkCount,
	}
}

func (s *contentAddressableStorageServer) FindMissingBlobs(ctx context.Context, in *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	if len(in.BlobDigests) == 0 {
		return &remoteexecution.FindMissingBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigests[0].GetHash()))
	if err != nil {
		return nil, err
	}

	inDigests := digest.NewSetBuilder(len(in.BlobDigests))
	for _, inDigest := range in.BlobDigests {
		d, err := digestFunction.NewDigestFromProto(inDigest)
		if err != nil {
			return nil, err
		}
		inDigests.Add(d)
	}

	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	missing, err := cas.FindMissing(ctx, s.chunkStorage, s.chunkMappingStorage, params, inDigests.Build())
	if err != nil {
		return nil, err
	}

	outDigests := make([]*remoteexecution.Digest, 0, missing.Length())
	for _, outDigest := range missing.Items() {
		outDigests = append(outDigests, outDigest.GetProto())
	}

	return &remoteexecution.FindMissingBlobsResponse{
		MissingBlobDigests: outDigests,
	}, nil
}

func (s *contentAddressableStorageServer) readBlobFromBatch(ctx context.Context, blobDigest digest.Digest, params *remoteexecution.RepMaxCdcParams, compressor remoteexecution.Compressor_Value) ([]byte, error) {
	if blobDigest.GetSizeBytes() == 0 {
		switch compressor {
		case remoteexecution.Compressor_IDENTITY:
			return nil, nil
		case remoteexecution.Compressor_ZSTD:
			return []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x01, 0x00, 0x00}, nil
		default:
			panic("Unsupported compression algorithm should not be reachable")
		}
	}
	getChunkDigest := func(i int) digest.Digest { return blobDigest }
	chunkCount := 1
	if !cas.IsSingleChunk(params, blobDigest) {
		chunkMapping, err := s.chunkMappingStorage.Get(ctx, blobDigest)
		if err != nil {
			return nil, err
		}
		chunkCount = chunkMapping.Length()
		getChunkDigest = chunkMapping.GetDigestAtIndex
	}
	var buf bytes.Buffer
	buf.Grow(int(blobDigest.GetSizeBytes()))
	for i := range chunkCount {
		chunk, err := s.chunkStorage.Get(ctx, getChunkDigest(i))
		if err != nil {
			return nil, err
		}
		var data []byte
		switch compressor {
		case remoteexecution.Compressor_IDENTITY:
			data = chunk.GetBytes()
		case remoteexecution.Compressor_ZSTD:
			data, err = chunk.GetBytesCompressed(ctx)
		default:
			panic("Unsupported compression algorithm should not be reachable")
		}
		if err != nil {
			return nil, err
		}
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (s *contentAddressableStorageServer) BatchReadBlobs(ctx context.Context, in *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	if len(in.Digests) == 0 {
		return &remoteexecution.BatchReadBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.Digests[0].GetHash()))
	if err != nil {
		return nil, err
	}

	// TODO: Compensate for message overhead.
	bytesRemaining := s.maximumMessageSizeBytes
	digests := make([]digest.Digest, 0, len(in.Digests))
	for _, reqDigest := range in.Digests {
		digest, err := digestFunction.NewDigestFromProto(reqDigest)
		if err != nil {
			return nil, err
		}
		sizeBytes := digest.GetSizeBytes()
		if sizeBytes > bytesRemaining {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"Attempted to read a total of at least %d bytes, while a maximum of %d bytes is permitted",
				uint64(s.maximumMessageSizeBytes-bytesRemaining)+uint64(sizeBytes),
				s.maximumMessageSizeBytes,
			)
		}
		bytesRemaining -= sizeBytes
		digests = append(digests, digest)
	}

	response := &remoteexecution.BatchReadBlobsResponse{
		Responses: make([]*remoteexecution.BatchReadBlobsResponse_Response, 0, len(in.Digests)),
	}
	compressor := remoteexecution.Compressor_IDENTITY
	if slices.Contains(in.AcceptableCompressors, remoteexecution.Compressor_ZSTD) {
		compressor = remoteexecution.Compressor_ZSTD
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	for i, digest := range digests {
		data, err := s.readBlobFromBatch(ctx, digest, params, compressor)
		response.Responses = append(
			response.Responses,
			&remoteexecution.BatchReadBlobsResponse_Response{
				Digest:     in.Digests[i],
				Data:       data,
				Compressor: compressor,
				Status:     status.Convert(err).Proto(),
			},
		)
	}

	return response, nil
}

func (s *contentAddressableStorageServer) BatchUpdateBlobs(ctx context.Context, in *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	if len(in.Requests) == 0 {
		return &remoteexecution.BatchUpdateBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.Requests[0].Digest.GetHash()))
	if err != nil {
		return nil, err
	}

	response := &remoteexecution.BatchUpdateBlobsResponse{
		Responses: make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 0, len(in.Requests)),
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	for _, request := range in.Requests {
		digest, err := digestFunction.NewDigestFromProto(request.Digest)
		if err == nil {
			if digest.GetSizeBytes() == 0 {
				// Empty blob is always present in storage.
				err = nil
			} else if cas.IsSingleChunk(params, digest) {
				err = s.updateChunk(ctx, digest, request.Data, request.Compressor)
			} else {
				err = s.updateBlob(ctx, digest, request.Data, request.Compressor, params)
			}
		}
		response.Responses = append(response.Responses,
			&remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: request.Digest,
				Status: status.Convert(err).Proto(),
			})
	}
	return response, nil
}

func (s *contentAddressableStorageServer) updateChunk(ctx context.Context, d digest.Digest, data []byte, compressor remoteexecution.Compressor_Value) error {
	var decompressedData []byte
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		decompressedData = data
	case remoteexecution.Compressor_ZSTD:
		decoder, err := s.zstdPool.NewDecoder(ctx, bytes.NewReader(data))
		if err != nil {
			return util.StatusWrap(err, "Failed to acquire ZSTD decoder")
		}
		r := io.LimitReader(decoder, d.GetSizeBytes()+1)
		if decompressedData, err = io.ReadAll(r); err != nil {
			decoder.Close()
			return util.StatusWrap(err, "Failed to decompress ZSTD data")
		}
		decoder.Close()
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}
	generator := d.GetDigestFunction().NewGenerator(d.GetSizeBytes())
	if _, err := generator.Write(decompressedData); err != nil {
		return status.Error(codes.Internal, "Could not compute digest of blob")
	}
	if actual := generator.Sum(); actual != d {
		return status.Errorf(codes.InvalidArgument, "Blob digest mismatch, advertised %s, actual %s", d, actual)
	}
	// We can't reuse the compression here as it comes from an untrusted
	// client. Even if it represents the correct blob it could still be
	// an arbitrarily poor compression.
	if err := s.chunkStorage.Put(ctx, d, chunk.NewChunk(s.zstdPool, decompressedData)); err != nil {
		return util.StatusWrap(err, "Failed to save chunk")
	}
	return nil
}

func (s *contentAddressableStorageServer) updateBlob(ctx context.Context, d digest.Digest, data []byte, compressor remoteexecution.Compressor_Value, params *remoteexecution.RepMaxCdcParams) error {
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return s.readerPutter.PutReaderAt(ctx, d, bytes.NewReader(data), params)
	case remoteexecution.Compressor_ZSTD:
		decoder, err := s.zstdPool.NewDecoder(ctx, bytes.NewReader(data))
		if err != nil {
			return util.StatusWrap(err, "Failed to acquire ZSTD decoder")
		}
		defer decoder.Close()

		// Limit the amount of data that is read to one byte beyond the
		// advertised size, so that corrupt or malicious streams cannot
		// trigger unbounded decompression.
		r := io.LimitReader(decoder, d.GetSizeBytes()+1)
		return s.readerPutter.PutReader(ctx, d, r, params)
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}
}

func (contentAddressableStorageServer) GetTree(in *remoteexecution.GetTreeRequest, stream remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "This service does not support downloading directory trees")
}

func (s *contentAddressableStorageServer) registerChunkMapping(ctx context.Context, d digest.Digest, digests []*remoteexecution.Digest) error {
	if len(digests) < 2 {
		return status.Error(codes.Unimplemented, "This server does not implement trivial RegisterChunkMapping calls")
	}
	// Store the chunk mapping.
	chunkMapping, err := chunk.NewMappingFromProtoDigests(d.GetDigestFunction(), digests, uint64(d.GetSizeBytes()), false)
	if err != nil {
		return err
	}
	if err := s.chunkMappingStorage.Put(ctx, d, chunkMapping); err != nil {
		return util.StatusWrap(err, "Could not save chunk mapping for blob")
	}
	return nil
}

func (s *contentAddressableStorageServer) RegisterChunkMapping(stream remoteexecution.ContentAddressableStorage_RegisterChunkMappingServer) error {
	ctx := stream.Context()

	in, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "Did not receive initial message")
		}
		return err
	}
	if in.BlobDigest == nil {
		return status.Error(codes.InvalidArgument, "The first request does not contain a blob digest")
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(ctx, s.putChunkMappingAuthorizer, instanceName); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	blobDigestProto := in.BlobDigest

	var chunkDigests []*remoteexecution.Digest
	for {
		chunkDigests = append(chunkDigests, in.ChunkDigests...)
		if len(chunkDigests) > s.maximumChunkCount {
			return status.Errorf(
				codes.InvalidArgument,
				"Attempted to register a blob that consists of a total of at least %d chunks, while a maximum of %d chunks is permitted",
				len(chunkDigests),
				s.maximumChunkCount,
			)
		}

		in, err = stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	blobDigest, err := digestFunction.NewDigestFromProto(blobDigestProto)
	if err != nil {
		return err
	}

	if err := s.registerChunkMapping(ctx, blobDigest, chunkDigests); err != nil {
		return err
	}

	return stream.SendAndClose(&remoteexecution.RegisterChunkMappingResponse{
		BlobDigest: blobDigestProto,
	})
}

func (s *contentAddressableStorageServer) SpliceBlob(ctx context.Context, in *remoteexecution.SpliceBlobRequest) (*remoteexecution.SpliceBlobResponse, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		return nil, err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(ctx, s.putChunkMappingAuthorizer, instanceName); err != nil {
		return nil, util.StatusWrap(err, "Authorization")
	}

	if len(in.ChunkDigests) > s.maximumChunkCount {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Attempted to splice a total of at least %d chunks, while a maximum of %d chunks is permitted",
			len(in.ChunkDigests),
			s.maximumChunkCount,
		)
	}

	if err := s.registerChunkMapping(ctx, blobDigest, in.ChunkDigests); err != nil {
		return nil, err
	}

	return &remoteexecution.SpliceBlobResponse{
		BlobDigest: in.BlobDigest,
	}, nil
}

func (s *contentAddressableStorageServer) SplitBlob(ctx context.Context, in *remoteexecution.SplitBlobRequest) (*remoteexecution.SplitBlobResponse, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		return nil, err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(ctx, s.getChunkMappingAuthorizer, instanceName); err != nil {
		return nil, util.StatusWrap(err, "Authorization")
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	if cas.IsSingleChunk(params, blobDigest) {
		return nil, status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings")
	}
	chunkMapping, err := s.chunkMappingStorage.Get(ctx, blobDigest)
	if err != nil {
		return nil, err
	}

	return &remoteexecution.SplitBlobResponse{
		ChunkDigests:     chunkMapping.GetProtoDigests(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}, nil
}

func (s *contentAddressableStorageServer) GetChunkMapping(in *remoteexecution.GetChunkMappingRequest, stream remoteexecution.ContentAddressableStorage_GetChunkMappingServer) error {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		return err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(stream.Context(), s.getChunkMappingAuthorizer, instanceName); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(stream.Context(), instanceName)
	if err != nil {
		return err
	}

	if cas.IsSingleChunk(params, blobDigest) {
		return status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings")
	}
	chunkMapping, err := s.chunkMappingStorage.Get(stream.Context(), blobDigest)
	if err != nil {
		return err
	}

	protoDigests := chunkMapping.GetProtoDigests()

	batchSize := int(blobstore.RecommendedFindMissingDigestsCount)
	for len(protoDigests) > 0 {
		n := min(len(protoDigests), batchSize)
		if err := stream.Send(&remoteexecution.GetChunkMappingResponse{
			ChunkDigests:     protoDigests[:n],
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}); err != nil {
			return err
		}
		protoDigests = protoDigests[n:]
	}
	return nil
}
