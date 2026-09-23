package grpcservers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type contentAddressableStorageServer struct {
	chunkStorage            blobstore.BlobAccess[*chunk.Chunk]
	chunkListStorage        blobstore.BlobAccess[chunk.List]
	chunkListFetcher        chunk.ListFetcher
	cdcParametersFetcher    cdc.ParametersFetcher
	zstdPool                zstd.Pool
	maximumMessageSizeBytes int64
	maximumChunkCount       int
}

// NewContentAddressableStorageServer creates a GRPC service for serving
// the contents of a Bazel Content Addressable Storage (CAS) to Bazel.
func NewContentAddressableStorageServer(chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkListStorage blobstore.BlobAccess[chunk.List], cdcParametersFetcher cdc.ParametersFetcher, zstdPool zstd.Pool, maximumMessageSizeBytes int64, maximumChunkCount int) remoteexecution.ContentAddressableStorageServer {
	return &contentAddressableStorageServer{
		chunkStorage:            chunkStorage,
		chunkListStorage:        chunkListStorage,
		cdcParametersFetcher:    cdcParametersFetcher,
		zstdPool:                zstdPool,
		maximumMessageSizeBytes: maximumMessageSizeBytes,
		maximumChunkCount:       maximumChunkCount,
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
		digest, err := digestFunction.NewDigestFromProto(inDigest)
		if err != nil {
			return nil, err
		}
		inDigests.Add(digest)
	}

	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	missing, err := cas.FindMissing(ctx, s.chunkStorage, s.chunkListStorage, params, inDigests.Build())
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
	chunkList := chunk.List{
		Digests: []digest.Digest{blobDigest},
		Offsets: []uint64{0},
	}
	var err error
	if !cas.IsSingleChunk(params, blobDigest) {
		chunkList, err = s.chunkListStorage.Get(ctx, blobDigest)
		if err != nil {
			return nil, err
		}
	}
	var buf bytes.Buffer
	buf.Grow(int(blobDigest.GetSizeBytes()))
	for _, chunkDigest := range chunkList.Digests {
		chunk, err := s.chunkStorage.Get(ctx, chunkDigest)
		if err != nil {
			return nil, err
		}
		var data []byte
		switch compressor {
		case remoteexecution.Compressor_IDENTITY:
			data, err = chunk.GetBytes(ctx)
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
	for i := 0; i < len(digests); i++ {
		data, err := s.readBlobFromBatch(ctx, digests[i], params, compressor)
		response.Responses = append(
			response.Responses,
			&remoteexecution.BatchReadBlobsResponse_Response{
				Digest: in.Digests[i],
				Data:   data,
				Status: status.Convert(err).Proto(),
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
			err = s.updateBlob(ctx, digest, request.Data, request.Compressor, params)
		}
		response.Responses = append(response.Responses,
			&remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: request.Digest,
				Status: status.Convert(err).Proto(),
			})
	}
	return response, nil
}

func (s *contentAddressableStorageServer) updateBlob(ctx context.Context, d digest.Digest, data []byte, compressor remoteexecution.Compressor_Value, params *remoteexecution.RepMaxCdcParams) error {
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return cas.PutBytes(ctx, s.zstdPool, s.chunkStorage, s.chunkListStorage, params, d, data)
	case remoteexecution.Compressor_ZSTD:
		decoder, err := s.zstdPool.NewDecoder(ctx, bytes.NewReader(data))
		if err != nil {
			return util.StatusWrap(err, "Failed to acquire ZSTD decoder")
		}

		// Limit the amount of data that is read to one byte beyond the
		// advertised size, so that corrupt or malicious streams cannot
		// trigger unbounded decompression.
		data, err = io.ReadAll(io.LimitReader(decoder, d.GetSizeBytes()+1))
		decoder.Close()
		if err != nil {
			return util.StatusWrapWithCode(err, codes.InvalidArgument, "Failed to decompress blob")
		}
		return cas.PutBytes(ctx, s.zstdPool, s.chunkStorage, s.chunkListStorage, params, d, data)
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}
}

func (contentAddressableStorageServer) GetTree(in *remoteexecution.GetTreeRequest, stream remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "This service does not support downloading directory trees")
}

func (s *contentAddressableStorageServer) registerChunkMapping(ctx context.Context, d digest.Digest, chunkList chunk.List) error {
	if err := s.chunkListStorage.Put(ctx, d, chunkList); err != nil {
		return util.StatusWrap(err, "Could not save chunk list for blob")
	}
	return nil
}

// newChunkList constructs a chunk list from a sequence of chunk
// digests, computing the offset at which every chunk starts.
func newChunkList(digests []digest.Digest) chunk.List {
	chunkList := chunk.List{
		Digests: digests,
		Offsets: make([]uint64, len(digests)),
	}
	offset := uint64(0)
	for i, d := range digests {
		chunkList.Offsets[i] = offset
		offset += uint64(d.GetSizeBytes())
	}
	return chunkList
}

func (s *contentAddressableStorageServer) RegisterChunkMapping(stream remoteexecution.ContentAddressableStorage_RegisterChunkMappingServer) error {
	ctx := stream.Context()
	var digestFunction digest.Function
	var blobDigestProto *remoteexecution.Digest
	var chunkDigests []digest.Digest
	for i := 0; ; i++ {
		in, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if i == 0 {
			if in.BlobDigest == nil {
				return status.Error(codes.InvalidArgument, "The first request does not contain a blob digest")
			}
			instanceName, err := digest.NewInstanceName(in.InstanceName)
			if err != nil {
				return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
			}
			digestFunction, err = instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
			if err != nil {
				return err
			}
			blobDigestProto = in.BlobDigest
		}
		for _, digestProto := range in.ChunkDigests {
			chunkDigest, err := digestFunction.NewDigestFromProto(digestProto)
			if err != nil {
				return err
			}
			chunkDigests = append(chunkDigests, chunkDigest)
		}
		if len(chunkDigests) > s.maximumChunkCount {
			return status.Errorf(
				codes.InvalidArgument,
				"Attempted to splice a total of at least %d chunks, while a maximum of %d chunks is permitted",
				len(chunkDigests),
				s.maximumChunkCount,
			)
		}
	}
	if blobDigestProto == nil {
		return status.Error(codes.InvalidArgument, "The stream did not contain any requests")
	}
	blobDigest, err := digestFunction.NewDigestFromProto(blobDigestProto)
	if err != nil {
		return err
	}

	if err := s.registerChunkMapping(ctx, blobDigest, newChunkList(chunkDigests)); err != nil {
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

	if len(in.ChunkDigests) > s.maximumChunkCount {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Attempted to splice a total of at least %d chunks, while a maximum of %d chunks is permitted",
			len(in.ChunkDigests),
			s.maximumChunkCount,
		)
	}

	chunkDigests := make([]digest.Digest, len(in.ChunkDigests))
	for i, digestProto := range in.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(digestProto)
		if err != nil {
			return nil, err
		}
		chunkDigests[i] = chunkDigest
	}

	if err := s.registerChunkMapping(ctx, blobDigest, newChunkList(chunkDigests)); err != nil {
		return nil, err
	}

	return &remoteexecution.SpliceBlobResponse{
		BlobDigest: in.BlobDigest,
	}, nil
}

func (s *contentAddressableStorageServer) getChunkMapping(ctx context.Context, params *remoteexecution.RepMaxCdcParams, d digest.Digest) (chunk.List, error) {
	if cas.IsSingleChunk(params, d) {
		// Blobs that fit in a single chunk have no chunk lists in
		// storage, but one may be created trivially on the fly provided
		// the chunk exists.
		missing, err := cas.FindMissing(ctx, s.chunkStorage, s.chunkListStorage, params, d.ToSingletonSet())
		if err != nil {
			return chunk.List{}, util.StatusWrap(err, "Failed to check blob existence")
		}
		if !missing.Empty() {
			return chunk.List{}, status.Errorf(codes.NotFound, "Blob %s not found", d)
		}
		return chunk.List{Digests: []digest.Digest{d}, Offsets: []uint64{0}}, nil
	}

	return s.chunkListStorage.Get(ctx, d)
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
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	chunkList, err := s.getChunkMapping(ctx, params, blobDigest)
	if err != nil {
		return nil, err
	}

	chunkDigests := make([]*remoteexecution.Digest, len(chunkList.Digests))
	for i, chunkDigest := range chunkList.Digests {
		chunkDigests[i] = chunkDigest.GetProto()
	}

	return &remoteexecution.SplitBlobResponse{
		ChunkDigests:     chunkDigests,
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
	params, err := s.cdcParametersFetcher.FetchCDCParameters(stream.Context(), instanceName)
	if err != nil {
		return err
	}

	chunkList, err := s.getChunkMapping(stream.Context(), params, blobDigest)
	if err != nil {
		return err
	}

	chunkDigests := make([]*remoteexecution.Digest, len(chunkList.Digests))
	for i, chunkDigest := range chunkList.Digests {
		chunkDigests[i] = chunkDigest.GetProto()
	}

	batchSize := int(blobstore.RecommendedFindMissingDigestsCount)
	for len(chunkDigests) > 0 {
		n := batchSize
		if len(chunkDigests) < n {
			n = len(chunkDigests)
		}
		if err := stream.Send(&remoteexecution.GetChunkMappingResponse{
			ChunkDigests:     chunkDigests[:n],
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}); err != nil {
			return err
		}
		chunkDigests = chunkDigests[n:]
	}
	return nil
}
