package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	status_pb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestContentAddressableStorageServerBatchReadBlobsSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 123)
	digest2 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 234)
	digest3 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6", 345)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 123,
			},
			{
				Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
				SizeBytes: 234,
			},
			{
				Hash:      "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6",
				SizeBytes: 345,
			},
		},
		InstanceName: "ubuntu1804",
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)

	singleChunkParameters := cdc.Parameters{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cas.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("ubuntu1804")).Return(singleChunkParameters, nil).Times(3)
	a := make([]byte, 123)
	cas.EXPECT().FetchChunk(ctx, digest1).Return(a, nil)
	b := make([]byte, 234)
	cas.EXPECT().FetchChunk(ctx, digest2).Return(b, nil)
	cas.EXPECT().FetchChunk(ctx, digest3).Return(nil, status.Error(codes.NotFound, "The object you requested could not be found"))

	maximumMessageSizeBytes := 4 << 20
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, int64(maximumMessageSizeBytes))

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchReadBlobsResponse{
		Responses: []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest: &remoteexecution.Digest{
					Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
					SizeBytes: 123,
				},
				Data: a,
			},
			{
				Digest: &remoteexecution.Digest{
					Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
					SizeBytes: 234,
				},
				Data: b,
			},
			{
				Digest: &remoteexecution.Digest{
					Hash:      "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6",
					SizeBytes: 345,
				},
				Status: &status_pb.Status{
					Code:    int32(codes.NotFound),
					Message: "The object you requested could not be found",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchReadBlobsFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 123,
			},
			{
				Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
				SizeBytes: 234,
			},
		},
		InstanceName: "ubuntu1804",
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)

	maximumMessageSizeBytes := 200
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, int64(maximumMessageSizeBytes))

	_, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attempted to read a total of at least 357 bytes, while a maximum of 200 bytes is permitted"), err)
}

func TestContentAddressableStorageServerBatchUpdateBlobs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 5)
	digest2 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 5)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest1.GetProto(),
				Data:   []byte("Hello"),
			},
			{
				Digest: digest2.GetProto(),
				Data:   []byte("World"),
			},
		},
		InstanceName: "ubuntu1804",
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)

	cas.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(cdc.Parameters{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil).Times(2)
	cas.EXPECT().PutChunk(ctx, digest1, []byte("Hello")).Return(nil)
	cas.EXPECT().PutChunk(ctx, digest2, []byte("World")).Return(status.Error(codes.Internal, "Hard disk has a case of the Mondays"))

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, 4<<20)

	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
		Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
			{
				Digest: digest1.GetProto(),
			},
			{
				Digest: digest2.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.Internal),
					Message: "Hard disk has a case of the Mondays",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerFindMissingBlobs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 16)
	digest2 := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 256)

	request := &remoteexecution.FindMissingBlobsRequest{
		InstanceName: "my_instance_name",
		BlobDigests: []*remoteexecution.Digest{
			{Hash: digest1.GetHashString(), SizeBytes: digest1.GetSizeBytes()},
			{Hash: digest2.GetHashString(), SizeBytes: digest2.GetSizeBytes()},
		},
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)

	digests := digest.NewSetBuilder(2)
	digests.Add(digest1)
	digests.Add(digest2)

	cas.EXPECT().FindMissing(ctx, digests.Build()).Return(digest.EmptySet, nil)

	maximumMessageSizeBytes := 200
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, int64(maximumMessageSizeBytes))

	response, err := contentAddressableStorageServer.FindMissingBlobs(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetMissingBlobDigests())
}

func TestContentAddressableStorageServerSplitBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunk1Digest, err := digestFunction.NewDigestFromProto(&remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 8,
	})
	require.NoError(t, err)
	chunk2Digest, err := digestFunction.NewDigestFromProto(&remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 8,
	})
	require.NoError(t, err)

	cas := mock.NewMockContentAddressableStorage(ctrl)
	cas.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(cdc.Parameters{MinChunkSizeBytes: 4, HorizonSizeBytes: 8}, nil)
	cas.EXPECT().GetManifest(ctx, blobDigest).Return(
		chunklist.ChunkList{
			{Offset: 0, Digest: chunk1Digest},
			{Offset: 8, Digest: chunk2Digest},
		},
		nil,
	)

	maximumMessageSizeBytes := 200
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, int64(maximumMessageSizeBytes))

	response, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.ChunkDigests, 2)
	require.Equal(t, remoteexecution.ChunkingFunction_REP_MAX_CDC, response.ChunkingFunction)
}

func TestContentAddressableStorageServerSplitBlobSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)

	// The blob is small enough to fit in a single chunk. Its chunk
	// list is never consulted; the blob itself is the single chunk.
	cas.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(cdc.Parameters{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	blobDigest, err := digest.MustNewFunction("my_instance_name", remoteexecution.DigestFunction_SHA256).NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)
	cas.EXPECT().FindMissing(ctx, blobDigest.ToSingletonSet()).Return(digest.EmptySet, nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, 200)

	response, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.ChunkDigests, 1)
	testutil.RequireEqualProto(t, &remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 16,
	}, response.ChunkDigests[0])
	require.Equal(t, remoteexecution.ChunkingFunction_REP_MAX_CDC, response.ChunkingFunction)
}

func TestContentAddressableStorageServerSplitBlobNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)

	// A single-chunk blob that does not exist.
	cas.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(cdc.Parameters{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	blobDigest, err := digest.MustNewFunction("my_instance_name", remoteexecution.DigestFunction_SHA256).NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)
	cas.EXPECT().FindMissing(ctx, blobDigest.ToSingletonSet()).Return(blobDigest.ToSingletonSet(), nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, 200)

	_, err = contentAddressableStorageServer.SplitBlob(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob 1-409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49-16-my_instance_name not found"), err)
}

func TestContentAddressableStorageServerSpliceBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SpliceBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		ChunkDigests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 8,
			},
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 8,
			},
		},
		InstanceName: "my_instance_name",
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunk1Digest, err := digestFunction.NewDigestFromProto(request.ChunkDigests[0])
	require.NoError(t, err)
	chunk2Digest, err := digestFunction.NewDigestFromProto(request.ChunkDigests[1])
	require.NoError(t, err)

	expectedChunkList := chunklist.ChunkList{
		{Offset: 0, Digest: chunk1Digest},
		{Offset: 8, Digest: chunk2Digest},
	}

	cas := mock.NewMockContentAddressableStorage(ctrl)
	cas.EXPECT().PutManifest(ctx, blobDigest, expectedChunkList).Return(nil)

	maximumMessageSizeBytes := 200

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(cas, int64(maximumMessageSizeBytes))
	response, err := contentAddressableStorageServer.SpliceBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, request.BlobDigest, response.BlobDigest)
}
