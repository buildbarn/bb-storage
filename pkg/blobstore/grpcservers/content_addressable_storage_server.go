package grpcservers

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type contentAddressableStorageServer struct {
	contentAddressableStorage cas.ContentAddressableStorage
	maximumMessageSizeBytes   int64
}

// NewContentAddressableStorageServer creates a GRPC service for serving
// the contents of a Bazel Content Addressable Storage (CAS) to Bazel.
func NewContentAddressableStorageServer(contentAddressableStorage cas.ContentAddressableStorage, maximumMessageSizeBytes int64) remoteexecution.ContentAddressableStorageServer {
	return &contentAddressableStorageServer{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
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

	missing, err := s.contentAddressableStorage.FindMissing(ctx, inDigests.Build())
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
	for i, reqDigest := range in.Digests {
		data, err := cas.GetBytes(
			ctx,
			s.contentAddressableStorage,
			digests[i],
		)
		response.Responses = append(response.Responses, &remoteexecution.BatchReadBlobsResponse_Response{
			Digest: reqDigest,
			Data:   data,
			Status: status.Convert(err).Proto(),
		})
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
	for _, request := range in.Requests {
		digest, err := digestFunction.NewDigestFromProto(request.Digest)
		if err == nil {
			err = cas.PutBytes(
				ctx,
				s.contentAddressableStorage,
				digest,
				request.Data,
			)
		}
		response.Responses = append(response.Responses,
			&remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: request.Digest,
				Status: status.Convert(err).Proto(),
			})
	}
	return response, nil
}

func (contentAddressableStorageServer) GetTree(in *remoteexecution.GetTreeRequest, stream remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "This service does not support downloading directory trees")
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

	chunkList := make(chunklist.ChunkList, 0, len(in.ChunkDigests))
	offset := uint64(0)
	for _, chunkDigestProto := range in.ChunkDigests {
		chunkDigest, err := digestFunction.NewDigestFromProto(chunkDigestProto)
		if err != nil {
			return nil, err
		}
		chunkList = append(chunkList, chunklist.Entry{Digest: chunkDigest, Offset: offset})
		offset += uint64(chunkDigest.GetSizeBytes())
	}

	if err := s.contentAddressableStorage.PutManifest(ctx, blobDigest, chunkList); err != nil {
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

	// Blobs stored as a single chunk have no chunk list; synthesize
	// the trivial answer, provided the blob exists.
	params, err := s.contentAddressableStorage.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	if cas.IsSingleChunk(params, blobDigest) {
		missing, err := s.contentAddressableStorage.FindMissing(ctx, blobDigest.ToSingletonSet())
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to check blob existence")
		}
		if !missing.Empty() {
			return nil, status.Errorf(codes.NotFound, "Blob %s not found", blobDigest)
		}
		return &remoteexecution.SplitBlobResponse{
			ChunkDigests:     []*remoteexecution.Digest{in.BlobDigest},
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}, nil
	}

	storedChunkList, err := s.contentAddressableStorage.GetManifest(ctx, blobDigest)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to decode chunk list")
	}

	chunkDigests := make([]*remoteexecution.Digest, 0, len(storedChunkList))
	for _, entry := range storedChunkList {
		chunkDigests = append(chunkDigests, entry.Digest.GetProto())
	}

	return &remoteexecution.SplitBlobResponse{
		ChunkDigests:     chunkDigests,
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}, nil
}
