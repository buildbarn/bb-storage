package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestContentAddressableStorageServerBatchReadBlobsSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	digest1 := digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123)
	digest2 := digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000002", 234)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			digest1.GetPartialDigest(),
			digest2.GetPartialDigest(),
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

	contentAddressableStorageServer := cas.NewContentAddressableStorageServer(contentAddressableStorage, 1<<16)

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	if err != nil {
		t.Errorf("%v", err)
	}
	require.Equal(t, &remoteexecution.BatchReadBlobsResponse{
		Responses: []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest: digest1.GetPartialDigest(),
				Data:   a,
				Status: status.Convert(nil).Proto(),
			},
			{
				Digest: digest2.GetPartialDigest(),
				Data:   b,
				Status: status.Convert(nil).Proto(),
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchReadBlobsFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	digest1 := digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000001", 123)
	digest2 := digest.MustNewDigest("ubuntu1804", "0000000000000000000000000000000000000000000000000000000000000002", 234)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			digest1.GetPartialDigest(),
			digest2.GetPartialDigest(),
		},
		InstanceName: "ubuntu1804",
	}

	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)

	contentAddressableStorageServer := cas.NewContentAddressableStorageServer(contentAddressableStorage, 200)

	_, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	require.Equal(t, status.Error(codes.InvalidArgument,
		"Attempted to read a total of 357 bytes, while a maximum of 200 bytes is permitted"),
		err)
}
