package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/cdc"
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

	chunkStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage.EXPECT().GetCapabilities(gomock.Any(), gomock.Any()).Return(
		&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 256 * 1024,
					HorizonSizeBytes:  8 * 256 * 1024,
				},
			},
		}, nil,
	).AnyTimes()

	a := make([]byte, 123)
	buf := buffer.NewValidatedBufferFromByteSlice(a)
	chunkStorage.EXPECT().Get(ctx, digest1).Return(buf)
	b := make([]byte, 234)
	buf2 := buffer.NewValidatedBufferFromByteSlice(b)
	chunkStorage.EXPECT().Get(ctx, digest2).Return(buf2)
	buf3 := buffer.NewBufferFromError(status.Error(codes.NotFound, "The object you requested could not be found"))
	chunkStorage.EXPECT().Get(ctx, digest3).Return(buf3)

	maximumMessageSizeBytes := 4 << 20
	casChunker := cdc.NewCasChunkingBlobAccess(chunkStorage, chunkListStorage, maximumMessageSizeBytes)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(casChunker, chunkListStorage, int64(maximumMessageSizeBytes))

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

	chunkStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage.EXPECT().GetCapabilities(gomock.Any(), gomock.Any()).Return(
		&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 64,
					HorizonSizeBytes:  8 * 64,
				},
			},
		}, nil,
	).AnyTimes()

	maximumMessageSizeBytes := 200
	casChunker := cdc.NewCasChunkingBlobAccess(chunkStorage, chunkListStorage, maximumMessageSizeBytes)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(casChunker, chunkListStorage, int64(maximumMessageSizeBytes))

	_, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attempted to read a total of at least 357 bytes, while a maximum of 200 bytes is permitted"), err)
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

	chunkStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage.EXPECT().GetCapabilities(gomock.Any(), gomock.Any()).Return(
		&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 64,
					HorizonSizeBytes:  8 * 64,
				},
			},
		}, nil,
	).AnyTimes()

	// Digest1 is small so will be routed directly to chunk storage,
	// while digest2 is large and will be routed to the chunk list
	// storage.
	chunkStorage.EXPECT().FindMissing(ctx, digest1.ToSingletonSet()).Return(digest.EmptySet, nil)
	chunkListStorage.EXPECT().FindMissing(ctx, digest2.ToSingletonSet()).Return(digest.EmptySet, nil)

	maximumMessageSizeBytes := 200
	casChunker := cdc.NewCasChunkingBlobAccess(chunkStorage, chunkListStorage, maximumMessageSizeBytes)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(casChunker, chunkListStorage, int64(maximumMessageSizeBytes))

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

	chunkStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage.EXPECT().GetCapabilities(gomock.Any(), gomock.Any()).Return(
		&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 64,
					HorizonSizeBytes:  8 * 64,
				},
			},
		}, nil,
	).AnyTimes()

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunkListStorage.EXPECT().Get(ctx, blobDigest).Return(
		buffer.NewProtoBufferFromProto(
			&remoteexecution.SplitBlobResponse{
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
			},
			buffer.UserProvided,
		),
	)

	maximumMessageSizeBytes := 200
	casChunker := cdc.NewCasChunkingBlobAccess(chunkStorage, chunkListStorage, maximumMessageSizeBytes)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(casChunker, chunkListStorage, int64(maximumMessageSizeBytes))

	_, err = contentAddressableStorageServer.SplitBlob(ctx, request)
	require.NoError(t, err)
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

	chunkStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage.EXPECT().GetCapabilities(gomock.Any(), gomock.Any()).Return(
		&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 64,
					HorizonSizeBytes:  8 * 64,
				},
			},
		}, nil,
	).AnyTimes()

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunkListStorage.EXPECT().Put(ctx, blobDigest, buffer.NewProtoBufferFromProto(&remoteexecution.SplitBlobResponse{
		ChunkDigests: request.ChunkDigests,
	}, buffer.UserProvided)).Return(nil)

	maximumMessageSizeBytes := 200
	casChunker := cdc.NewCasChunkingBlobAccess(chunkStorage, chunkListStorage, maximumMessageSizeBytes)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(casChunker, chunkListStorage, int64(maximumMessageSizeBytes))
	response, err := contentAddressableStorageServer.SpliceBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, request.BlobDigest, response.BlobDigest)
}
