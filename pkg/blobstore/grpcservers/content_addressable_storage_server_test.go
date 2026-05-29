package grpcservers_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
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

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

	a := make([]byte, 123)
	buf := buffer.NewValidatedBufferFromByteSlice(a)
	contentAddressableStorage.EXPECT().Get(ctx, digest1).Return(buf)
	b := make([]byte, 234)
	buf2 := buffer.NewValidatedBufferFromByteSlice(b)
	contentAddressableStorage.EXPECT().Get(ctx, digest2).Return(buf2)
	buf3 := buffer.NewBufferFromError(status.Error(codes.NotFound, "The object you requested could not be found"))
	contentAddressableStorage.EXPECT().Get(ctx, digest3).Return(buf3)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(contentAddressableStorage, nil, 4<<20)

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

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(contentAddressableStorage, nil, 200)

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

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)
	setBuilder := digest.NewSetBuilder(2)
	digestSet := setBuilder.Add(digest1).Add(digest2).Build()

	// Missing chunk lists is not an error, nor does it imply a missing
	// blob at this stage.
	contentAddressableStorage.EXPECT().FindMissing(ctx, digestSet).Return(digest.EmptySet, nil)
	chunkListStorage.EXPECT().GetCapabilities(ctx, digest1.GetInstanceName()).Return(
		&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				SplitBlobSupport:  true,
				SpliceBlobSupport: true,
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 64,
					HorizonSizeBytes:  128,
				},
			},
		},
		nil,
	)
	chunkListStorage.EXPECT().FindMissing(ctx, digest2.ToSingletonSet()).Return(digest2.ToSingletonSet(), nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(contentAddressableStorage, chunkListStorage, 200)

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

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)

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

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(contentAddressableStorage, chunkListStorage, 200)
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

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	chunkListStorage := mock.NewMockBlobAccess(ctrl)

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunkListStorage.EXPECT().Put(ctx, blobDigest, buffer.NewProtoBufferFromProto(&remoteexecution.SplitBlobResponse{
		ChunkDigests: request.ChunkDigests,
	}, buffer.UserProvided)).Return(nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(contentAddressableStorage, chunkListStorage, 200)
	response, err := contentAddressableStorageServer.SpliceBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, request.BlobDigest, response.BlobDigest)
}
