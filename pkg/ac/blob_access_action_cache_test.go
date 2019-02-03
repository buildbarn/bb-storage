package ac_test

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/ac"
	"github.com/buildbarn/bb-storage/pkg/mock"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBlobAccessActionCacheGetBackendError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	blobAccess := mock.NewMockBlobAccess(ctrl)
	actionCache := ac.NewBlobAccessActionCache(blobAccess)

	// Backend not being able to serve the object.
	blobAccess.EXPECT().Get(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})).Return(int64(0), nil, status.Error(codes.Internal, "S3 bucket unavailable"))
	_, err := actionCache.GetActionResult(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}))
	require.Equal(t, err, status.Error(codes.Internal, "S3 bucket unavailable"))
}

func TestBlobAccessActionCacheGetMalformed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	blobAccess := mock.NewMockBlobAccess(ctrl)
	actionCache := ac.NewBlobAccessActionCache(blobAccess)

	// Malformed object stored in the Action Cache should trigger
	// object deletion.
	blobAccess.EXPECT().Get(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})).Return(int64(11), ioutil.NopCloser(bytes.NewBuffer([]byte("Hello world"))), nil)
	blobAccess.EXPECT().Delete(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})).Return(nil)
	_, err := actionCache.GetActionResult(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}))
	require.Equal(t, err, status.Error(codes.NotFound, "Failed to unmarshal message: proto: can't skip unknown wire type 4"))
}

func TestBlobAccessActionCacheGetSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	blobAccess := mock.NewMockBlobAccess(ctrl)
	actionCache := ac.NewBlobAccessActionCache(blobAccess)

	// Well formed Protobuf that can be deserialized properly.
	blobAccess.EXPECT().Get(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		})).Return(int64(134), ioutil.NopCloser(bytes.NewBuffer([]byte{
		0x12, 0x83, 0x01, 0x0a, 0x3a, 0x62, 0x61, 0x7a,
		0x65, 0x6c, 0x2d, 0x6f, 0x75, 0x74, 0x2f, 0x6b,
		0x38, 0x2d, 0x66, 0x61, 0x73, 0x74, 0x62, 0x75,
		0x69, 0x6c, 0x64, 0x2f, 0x62, 0x69, 0x6e, 0x2f,
		0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73,
		0x2f, 0x65, 0x70, 0x73, 0x74, 0x6f, 0x70, 0x64,
		0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x74,
		0x65, 0x73, 0x74, 0x2e, 0x70, 0x64, 0x66, 0x12,
		0x45, 0x0a, 0x40, 0x33, 0x38, 0x63, 0x61, 0x64,
		0x62, 0x30, 0x36, 0x66, 0x62, 0x36, 0x65, 0x34,
		0x61, 0x65, 0x35, 0x64, 0x30, 0x62, 0x61, 0x65,
		0x33, 0x39, 0x65, 0x30, 0x62, 0x38, 0x61, 0x38,
		0x66, 0x30, 0x63, 0x31, 0x34, 0x30, 0x38, 0x36,
		0x63, 0x33, 0x35, 0x61, 0x63, 0x32, 0x65, 0x62,
		0x31, 0x65, 0x31, 0x34, 0x34, 0x66, 0x39, 0x37,
		0x66, 0x63, 0x34, 0x37, 0x34, 0x35, 0x61, 0x35,
		0x33, 0x38, 0x36, 0x10, 0x84, 0x26,
	})), nil)
	actionResult, err := actionCache.GetActionResult(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}))
	require.NoError(t, err)
	require.Equal(t, &remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "38cadb06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
	}, actionResult)
}

func TestBlobAccessActionCachePutMalformed(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	blobAccess := mock.NewMockBlobAccess(ctrl)
	actionCache := ac.NewBlobAccessActionCache(blobAccess)

	// Malformed string in message cannot be serialized.
	err := actionCache.PutActionResult(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}),
		&remoteexecution.ActionResult{
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: string([]byte{0xff}),
				},
			},
		})
	require.Equal(t, err, status.Error(codes.InvalidArgument, "Failed to marshal message: proto: field \"build.bazel.remote.execution.v2.OutputFile.Path\" contains invalid UTF-8"))
}

func TestBlobAccessActionCachePutSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	blobAccess := mock.NewMockBlobAccess(ctrl)
	actionCache := ac.NewBlobAccessActionCache(blobAccess)

	// Well formed Protobuf that can be serialized properly.
	blobAccess.EXPECT().Put(
		ctx,
		util.MustNewDigest(
			"debian8",
			&remoteexecution.Digest{
				Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
				SizeBytes: 11,
			}),
		int64(134),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
		buf, err := ioutil.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, []byte{
			0x12, 0x83, 0x01, 0x0a, 0x3a, 0x62, 0x61, 0x7a,
			0x65, 0x6c, 0x2d, 0x6f, 0x75, 0x74, 0x2f, 0x6b,
			0x38, 0x2d, 0x66, 0x61, 0x73, 0x74, 0x62, 0x75,
			0x69, 0x6c, 0x64, 0x2f, 0x62, 0x69, 0x6e, 0x2f,
			0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73,
			0x2f, 0x65, 0x70, 0x73, 0x74, 0x6f, 0x70, 0x64,
			0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x74,
			0x65, 0x73, 0x74, 0x2e, 0x70, 0x64, 0x66, 0x12,
			0x45, 0x0a, 0x40, 0x33, 0x38, 0x63, 0x61, 0x64,
			0x62, 0x30, 0x36, 0x66, 0x62, 0x36, 0x65, 0x34,
			0x61, 0x65, 0x35, 0x64, 0x30, 0x62, 0x61, 0x65,
			0x33, 0x39, 0x65, 0x30, 0x62, 0x38, 0x61, 0x38,
			0x66, 0x30, 0x63, 0x31, 0x34, 0x30, 0x38, 0x36,
			0x63, 0x33, 0x35, 0x61, 0x63, 0x32, 0x65, 0x62,
			0x31, 0x65, 0x31, 0x34, 0x34, 0x66, 0x39, 0x37,
			0x66, 0x63, 0x34, 0x37, 0x34, 0x35, 0x61, 0x35,
			0x33, 0x38, 0x36, 0x10, 0x84, 0x26,
		}, buf)
		require.NoError(t, r.Close())
		return nil
	})
	err := actionCache.PutActionResult(ctx, util.MustNewDigest(
		"debian8",
		&remoteexecution.Digest{
			Hash:      "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c",
			SizeBytes: 11,
		}),
		&remoteexecution.ActionResult{
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
					Digest: &remoteexecution.Digest{
						Hash:      "38cadb06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
						SizeBytes: 4868,
					},
				},
			},
		})
	require.NoError(t, err)
}
