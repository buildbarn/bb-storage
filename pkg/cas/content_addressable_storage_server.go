package cas

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type contentAddressableStorageServer struct {
	contentAddressableStorage blobstore.BlobAccess
	maximumMessageSizeBytes   int64
}

// NewContentAddressableStorageServer creates a GRPC service for serving
// the contents of a Bazel Content Addressable Storage (CAS) to Bazel.
func NewContentAddressableStorageServer(contentAddressableStorage blobstore.BlobAccess, maximumMessageSizeBytes int64) remoteexecution.ContentAddressableStorageServer {
	return &contentAddressableStorageServer{
		contentAddressableStorage: contentAddressableStorage,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
	}
}

func (s *contentAddressableStorageServer) FindMissingBlobs(ctx context.Context, in *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	inDigests := digest.NewSetBuilder()
	for _, partialDigest := range in.BlobDigests {
		digest, err := digest.NewDigestFromPartialDigest(in.InstanceName, partialDigest)
		if err != nil {
			return nil, err
		}
		inDigests.Add(digest)
	}
	outDigests, err := s.contentAddressableStorage.FindMissing(ctx, inDigests.Build())
	if err != nil {
		return nil, err
	}
	partialDigests := make([]*remoteexecution.Digest, 0, outDigests.Length())
	for _, outDigest := range outDigests.Items() {
		partialDigests = append(partialDigests, outDigest.GetPartialDigest())
	}
	return &remoteexecution.FindMissingBlobsResponse{
		MissingBlobDigests: partialDigests,
	}, nil
}

func (s *contentAddressableStorageServer) BatchReadBlobs(ctx context.Context, in *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	bytesRemaining := s.maximumMessageSizeBytes
	digests := make([]digest.Digest, 0, len(in.Digests))
	for _, reqDigest := range in.Digests {
		digest, err := digest.NewDigestFromPartialDigest(in.InstanceName, reqDigest)
		if err != nil {
			return nil, err
		}
		sizeBytes := digest.GetSizeBytes()
		if sizeBytes > bytesRemaining {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"Attempted to read a total of at least %d bytes, while a maximum of %d bytes is permitted",
				uint64(s.maximumMessageSizeBytes-bytesRemaining)+uint64(sizeBytes),
				s.maximumMessageSizeBytes)
		}
		bytesRemaining -= sizeBytes
		digests = append(digests, digest)
	}

	var response remoteexecution.BatchReadBlobsResponse
	for i, reqDigest := range in.Digests {
		data, err := s.contentAddressableStorage.Get(
			ctx,
			digests[i]).ToByteSlice(int(digests[i].GetSizeBytes()))
		response.Responses = append(response.Responses, &remoteexecution.BatchReadBlobsResponse_Response{
			Digest: reqDigest,
			Data:   data,
			Status: status.Convert(err).Proto(),
		})
	}

	return &response, nil
}

func (s *contentAddressableStorageServer) BatchUpdateBlobs(ctx context.Context, in *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	var response remoteexecution.BatchUpdateBlobsResponse
	for _, request := range in.Requests {
		digest, err := digest.NewDigestFromPartialDigest(in.InstanceName, request.Digest)
		if err == nil {
			err = s.contentAddressableStorage.Put(
				ctx,
				digest,
				buffer.NewCASBufferFromByteSlice(digest, request.Data, buffer.UserProvided))
		}
		response.Responses = append(response.Responses,
			&remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: request.Digest,
				Status: status.Convert(err).Proto(),
			})
	}

	return &response, nil
}

func (s *contentAddressableStorageServer) GetTree(in *remoteexecution.GetTreeRequest, stream remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "This service does not support downloading directory trees")
}
