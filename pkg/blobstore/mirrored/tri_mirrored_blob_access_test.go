package mirrored_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maximumMessageSizeBytes = 8192 // for testing only
)

var (
	// An example ActionResult message that are used in a variety of
	// the unit tests.  Shamelessly adapted from buffer/example_test.go.
	testActionResultMessage = remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "38cadb06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
	}
	testActionResultBytes = []byte{
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
	}
	testActionResultByteSize = 134
	testActionResultDigest   = digest.MustNewDigest(
		"qux",
		remoteexecution.DigestFunction_MD5,
		"d555bf579673a15bb6301f4b2f0593a8",
		int64(testActionResultByteSize))

	// An invalidated version of the previous ActionResult message.
	// When bazel receives the message, it will note that the exit code
	// was nonzero, and force the action to be re-executed.
	testInvalidatedActionResultMessage = remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "38cadb06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
		ExitCode: 667,
	}
	testInvalidatedActionResultBytes = []byte{
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
		0x33, 0x38, 0x36, 0x10, 0x84, 0x26, 0x20, 0x9b,
		0x05,
	}
	testInvalidatedActionResultByteSize = 137

	// A conflicting version of the original ActionResult message. (The hash
	// is different.)
	testConflictingActionResultMessage1 = remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "1234db06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
	}
	testConflictingActionResultBytes1 = []byte{
		0x12, 0x83, 0x01, 0x0a, 0x3a, 0x62, 0x61, 0x7a,
		0x65, 0x6c, 0x2d, 0x6f, 0x75, 0x74, 0x2f, 0x6b,
		0x38, 0x2d, 0x66, 0x61, 0x73, 0x74, 0x62, 0x75,
		0x69, 0x6c, 0x64, 0x2f, 0x62, 0x69, 0x6e, 0x2f,
		0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73,
		0x2f, 0x65, 0x70, 0x73, 0x74, 0x6f, 0x70, 0x64,
		0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x74,
		0x65, 0x73, 0x74, 0x2e, 0x70, 0x64, 0x66, 0x12,
		0x45, 0x0a, 0x40, 0x31, 0x32, 0x33, 0x34, 0x64,
		0x62, 0x30, 0x36, 0x66, 0x62, 0x36, 0x65, 0x34,
		0x61, 0x65, 0x35, 0x64, 0x30, 0x62, 0x61, 0x65,
		0x33, 0x39, 0x65, 0x30, 0x62, 0x38, 0x61, 0x38,
		0x66, 0x30, 0x63, 0x31, 0x34, 0x30, 0x38, 0x36,
		0x63, 0x33, 0x35, 0x61, 0x63, 0x32, 0x65, 0x62,
		0x31, 0x65, 0x31, 0x34, 0x34, 0x66, 0x39, 0x37,
		0x66, 0x63, 0x34, 0x37, 0x34, 0x35, 0x61, 0x35,
		0x33, 0x38, 0x36, 0x10, 0x84, 0x26,
	}
	testConflictingActionResultByteSize1 = 134

	// The invalidated version of the previous conflicing ActionResult message.
	testInvalidatedConflictingActionResultMessage1 = remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "1234db06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
		ExitCode: 667,
	}
	testInvalidatedConflictingActionResultBytes1 = []byte{
		0x12, 0x83, 0x01, 0x0a, 0x3a, 0x62, 0x61, 0x7a,
		0x65, 0x6c, 0x2d, 0x6f, 0x75, 0x74, 0x2f, 0x6b,
		0x38, 0x2d, 0x66, 0x61, 0x73, 0x74, 0x62, 0x75,
		0x69, 0x6c, 0x64, 0x2f, 0x62, 0x69, 0x6e, 0x2f,
		0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73,
		0x2f, 0x65, 0x70, 0x73, 0x74, 0x6f, 0x70, 0x64,
		0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x74,
		0x65, 0x73, 0x74, 0x2e, 0x70, 0x64, 0x66, 0x12,
		0x45, 0x0a, 0x40, 0x31, 0x32, 0x33, 0x34, 0x64,
		0x62, 0x30, 0x36, 0x66, 0x62, 0x36, 0x65, 0x34,
		0x61, 0x65, 0x35, 0x64, 0x30, 0x62, 0x61, 0x65,
		0x33, 0x39, 0x65, 0x30, 0x62, 0x38, 0x61, 0x38,
		0x66, 0x30, 0x63, 0x31, 0x34, 0x30, 0x38, 0x36,
		0x63, 0x33, 0x35, 0x61, 0x63, 0x32, 0x65, 0x62,
		0x31, 0x65, 0x31, 0x34, 0x34, 0x66, 0x39, 0x37,
		0x66, 0x63, 0x34, 0x37, 0x34, 0x35, 0x61, 0x35,
		0x33, 0x38, 0x36, 0x10, 0x84, 0x26, 0x20, 0x9b,
		0x05,
	}
	testInvalidatedConflictingActionResultByteSize1 = 137

	// A second conflicting version of the original ActionResult message with a
	// different hash value than either of the previous messages.
	testConflictingActionResultMessage2 = remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "4567db06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
	}
	testConflictingActionResultBytes2 = []byte{
		0x12, 0x83, 0x01, 0x0a, 0x3a, 0x62, 0x61, 0x7a,
		0x65, 0x6c, 0x2d, 0x6f, 0x75, 0x74, 0x2f, 0x6b,
		0x38, 0x2d, 0x66, 0x61, 0x73, 0x74, 0x62, 0x75,
		0x69, 0x6c, 0x64, 0x2f, 0x62, 0x69, 0x6e, 0x2f,
		0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73,
		0x2f, 0x65, 0x70, 0x73, 0x74, 0x6f, 0x70, 0x64,
		0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x74,
		0x65, 0x73, 0x74, 0x2e, 0x70, 0x64, 0x66, 0x12,
		0x45, 0x0a, 0x40, 0x34, 0x35, 0x36, 0x37, 0x64,
		0x62, 0x30, 0x36, 0x66, 0x62, 0x36, 0x65, 0x34,
		0x61, 0x65, 0x35, 0x64, 0x30, 0x62, 0x61, 0x65,
		0x33, 0x39, 0x65, 0x30, 0x62, 0x38, 0x61, 0x38,
		0x66, 0x30, 0x63, 0x31, 0x34, 0x30, 0x38, 0x36,
		0x63, 0x33, 0x35, 0x61, 0x63, 0x32, 0x65, 0x62,
		0x31, 0x65, 0x31, 0x34, 0x34, 0x66, 0x39, 0x37,
		0x66, 0x63, 0x34, 0x37, 0x34, 0x35, 0x61, 0x35,
		0x33, 0x38, 0x36, 0x10, 0x84, 0x26,
	}
	testConflictingActionResultByteSize2 = 134

	// The invalidated version of the second conflicing ActionResult message.
	testInvalidatedConflictingActionResultMessage2 = remoteexecution.ActionResult{
		OutputFiles: []*remoteexecution.OutputFile{
			{
				Path: "bazel-out/k8-fastbuild/bin/packages/epstopdf-base_test.pdf",
				Digest: &remoteexecution.Digest{
					Hash:      "4567db06fb6e4ae5d0bae39e0b8a8f0c14086c35ac2eb1e144f97fc4745a5386",
					SizeBytes: 4868,
				},
			},
		},
		ExitCode: 667,
	}
	testInvalidatedConflictingActionResultBytes2 = []byte{
		0x12, 0x83, 0x01, 0x0a, 0x3a, 0x62, 0x61, 0x7a,
		0x65, 0x6c, 0x2d, 0x6f, 0x75, 0x74, 0x2f, 0x6b,
		0x38, 0x2d, 0x66, 0x61, 0x73, 0x74, 0x62, 0x75,
		0x69, 0x6c, 0x64, 0x2f, 0x62, 0x69, 0x6e, 0x2f,
		0x70, 0x61, 0x63, 0x6b, 0x61, 0x67, 0x65, 0x73,
		0x2f, 0x65, 0x70, 0x73, 0x74, 0x6f, 0x70, 0x64,
		0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0x5f, 0x74,
		0x65, 0x73, 0x74, 0x2e, 0x70, 0x64, 0x66, 0x12,
		0x45, 0x0a, 0x40, 0x34, 0x35, 0x36, 0x37, 0x64,
		0x62, 0x30, 0x36, 0x66, 0x62, 0x36, 0x65, 0x34,
		0x61, 0x65, 0x35, 0x64, 0x30, 0x62, 0x61, 0x65,
		0x33, 0x39, 0x65, 0x30, 0x62, 0x38, 0x61, 0x38,
		0x66, 0x30, 0x63, 0x31, 0x34, 0x30, 0x38, 0x36,
		0x63, 0x33, 0x35, 0x61, 0x63, 0x32, 0x65, 0x62,
		0x31, 0x65, 0x31, 0x34, 0x34, 0x66, 0x39, 0x37,
		0x66, 0x63, 0x34, 0x37, 0x34, 0x35, 0x61, 0x35,
		0x33, 0x38, 0x36, 0x10, 0x84, 0x26, 0x20, 0x9b,
		0x05,
	}
	testInvalidatedConflictingActionResultByteSize2 = 137
)

func TestTriMirroredCASBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	backendC := mock.NewMockBlobAccess(ctrl)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("Success", func(t *testing.T) {
		// All backends should receive the same read request.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("NotFoundAll", func(t *testing.T) {
		// Simulate the case where a blob is not present in any of
		// the backends.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("RepairSuccessOneMissing", func(t *testing.T) {
		// The blob is only present in two backends. It
		// will get synchronized into the third.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})

	t.Run("RepairSuccessTwoMissing", func(t *testing.T) {
		// The blob is only present in the one backend. It
		// will get synchronized into the second and third.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})

	t.Run("RepairFailureTwoMissingGetSucceeds", func(t *testing.T) {
		// The blob is only present in the one backend. We will try to
		// replicated it to the second and thirdi, but if this fails,
		// the read hsould still succeed.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})
}

func TestTriMirroredACBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	backendC := mock.NewMockBlobAccess(ctrl)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("Success", func(t *testing.T) {
		// All backends should receive the same read request.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, testActionResultBytes, data)
	})

	t.Run("NotFoundAll", func(t *testing.T) {
		// Simulate the case where a blob is not present in any of
		// the backends.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("RepairSuccessOneMissing", func(t *testing.T) {
		// The blob is only present in two backends. It
		// will get synchronized into the third.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, testActionResultBytes, data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})

	t.Run("RepairFailureTwoMissing", func(t *testing.T) {
		// The blob is only present in the one backend. It
		// will get invalidated.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedActionResultBytes, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, testInvalidatedActionResultBytes, data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})

	t.Run("RepairFailureTwoMismatch", func(t *testing.T) {
		// The blob is only present in two backends, but don't match.
		// They will get invalidated.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedConflictingActionResultBytes1, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("RepairFailureAllMismatch", func(t *testing.T) {
		// The blob is only present in all backends, but none match.
		// They will get invalidated.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes2))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedConflictingActionResultBytes1, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedConflictingActionResultBytes2, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	//
	// These next three tests exercise different combinations of backends matching the AC entries.
	// This will exercise the quorum matching logic.
	//
	t.Run("RepairFailureTwoMatchOneMismatch_Pattern101", func(t *testing.T) {
		// The blob is present in all backends, but matches in only the first and third backends, so we have a quorum.
		// The blob will be replicated to the second backend.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, testActionResultBytes, data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})

	t.Run("RepairFailureTwoMatchOneMismatch_Pattern110", func(t *testing.T) {
		// The blob is present in all backends, but matches in only the first two backends, so we have a quorum.
		// The blob will be replicated to the third backend.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, testActionResultBytes, data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})

	t.Run("RepairFailureTwoMatchOneMismatch_Pattern011", func(t *testing.T) {
		// The blob is present in all backends, but matches in only the last two backends, so we have a quorum.
		// The blob will be replicated to the first backend.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(maximumMessageSizeBytes)
		require.NoError(t, err)
		require.Equal(t, testActionResultBytes, data)
		time.Sleep(2 * time.Second) // Give the background go routine time to replicate the buffer
	})
}

func TestTriMirroredCASBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	backendC := mock.NewMockBlobAccess(ctrl)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("Success", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("OneBackendFails", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("TwoBackendsFail", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		require.Equal(
			t,
			fmt.Errorf("Too many failures: Backend A: rpc error: code = Internal desc = Server on fire, Backend B: rpc error: code = Internal desc = Server on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("AllBackendsFail", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)
		require.Equal(
			t,
			fmt.Errorf("Too many failures: Backend A: rpc error: code = Internal desc = Server on fire, Backend B: rpc error: code = Internal desc = Server on fire, Backend C: rpc error: code = Internal desc = Server on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})
}

func TestTriMirroredACBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	backendC := mock.NewMockBlobAccess(ctrl)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("Success", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		// These Get requests are from the AC tri-mirror verifying that a quorum exists and the Puts didn't
		// race with other Puts or Gets.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(testActionResultBytes)))
	})

	t.Run("OneBackendFails", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(testActionResultBytes)))
	})

	t.Run("TwoBackendsFail", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testInvalidatedActionResultBytes, data)
				return nil
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		require.Equal(
			t,
			fmt.Errorf("Too many failures: Backend A: rpc error: code = Internal desc = Server on fire, Backend B: rpc error: code = Internal desc = Server on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(testActionResultBytes)))
	})

	t.Run("AllBackendsFail", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		require.Equal(
			t,
			fmt.Errorf("Too many failures: Backend A: rpc error: code = Internal desc = Server on fire, Backend B: rpc error: code = Internal desc = Server on fire, Backend C: rpc error: code = Internal desc = Server on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(testActionResultBytes)))
	})

	t.Run("RetryAfterRaceSucceeds", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes2))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(testActionResultBytes)))
	})

	t.Run("RetryAfterRaceFails", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes2))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendC.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, testActionResultBytes, data)
				return nil
			})
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes1))
		backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testConflictingActionResultBytes2))
		backendC.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))

		blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_ACTION_CACHE)
		err := blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice(testActionResultBytes))
		testutil.RequireEqualStatus(t, status.Error(codes.Aborted, "Race recovery failed"), err)
	})
}

func TestTriMirroredCASBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	backendC := mock.NewMockBlobAccess(ctrl)
	digestNone := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	digestA := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	digestB := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2", 9)
	digestC := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "c0c044d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5c0c02", 9)
	digestAll := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "9c6079651d4062b6811f93061cb6a768a60e51d714bddffee99b1173c6580580", 5)
	allDigests := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestB).Add(digestC).Add(digestAll).Build()
	//onlyOnA := digestA.ToSingletonSet()
	//onlyOnB := digestB.ToSingletonSet()
	//onlyOnC := digestC.ToSingletonSet()
	missingFromA := digest.NewSetBuilder().Add(digestNone).Add(digestB).Add(digestC).Build()
	missingFromB := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestC).Build()
	missingFromC := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestB).Build()
	blobAccess := mirrored.NewTriMirroredBlobAccess(backendA, backendB, backendC, pb.StorageType_CASTORE)

	t.Run("Success", func(t *testing.T) {
		// Listings of all backends should be requested.
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(missingFromB, nil)
		backendC.EXPECT().FindMissing(ctx, allDigests).Return(missingFromC, nil)
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from A")))
		backendB.EXPECT().Put(gomock.Any(), digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from A"), data)
				return nil
			})
		backendB.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendA.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from A")))
		backendC.EXPECT().Put(gomock.Any(), digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from A"), data)
				return nil
			})
		backendA.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendC.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendC.EXPECT().Get(ctx, digestC).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from C")))
		backendA.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from C"), data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from C"), data)
				return nil
			})

		// The intersection of missing blobs in the backends
		// should be returned.
		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		expectedMissing := digestNone.ToSingletonSet()
		require.Equal(t, expectedMissing, missing)
	})

	t.Run("FindMissingErrorOneBackend", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(missingFromB, nil)
		backendC.EXPECT().FindMissing(ctx, allDigests).Return(missingFromC, nil)
		backendB.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendC.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendC.EXPECT().Get(ctx, digestC).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from C")))
		backendB.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from C"), data)
				return nil
			})

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		expectedMissing := digest.NewSetBuilder().Add(digestNone).Add(digestA).Build()
		require.Equal(t, expectedMissing, missing)
	})

	t.Run("FindMissingErrorTwoBackends", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		backendC.EXPECT().FindMissing(ctx, allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, fmt.Errorf("Too many failures: Backend B: rpc error: code = Internal desc = Server on fire, Backend C: rpc error: code = Internal desc = Server on fire"), err)
	})

	t.Run("FindMissingErrorAllBackends", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		backendC.EXPECT().FindMissing(ctx, allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, fmt.Errorf("Too many failures: Backend A: rpc error: code = Internal desc = Server on fire, Backend B: rpc error: code = Internal desc = Server on fire, Backend C: rpc error: code = Internal desc = Server on fire"), err)
	})

	t.Run("FindMissingReplicationFailedButStillHaveQuorum", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(missingFromB, nil)
		backendC.EXPECT().FindMissing(ctx, allDigests).Return(missingFromC, nil)
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from A")))
		backendB.EXPECT().Put(gomock.Any(), digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from A"), data)
				return nil
			})
		backendB.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendA.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from A")))
		backendC.EXPECT().Put(gomock.Any(), digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from A"), data)
				return nil
			})
		backendA.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendC.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendC.EXPECT().Get(ctx, digestC).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from C")))
		backendA.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from C"), data)
				return nil
			})

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		expectedMissing := digestNone.ToSingletonSet()
		require.Equal(t, expectedMissing, missing)
	})

	t.Run("FindMissingReplicationFailed", func(t *testing.T) {
		backendA.EXPECT().FindMissing(ctx, allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(ctx, allDigests).Return(missingFromB, nil)
		backendC.EXPECT().FindMissing(ctx, allDigests).Return(missingFromC, nil)
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from A")))
		backendB.EXPECT().Put(gomock.Any(), digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from A"), data)
				return nil
			})
		backendB.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendA.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendA.EXPECT().Get(ctx, digestA).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from A")))
		backendC.EXPECT().Put(gomock.Any(), digestA, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from A"), data)
				return nil
			})
		backendA.EXPECT().Get(ctx, digestB).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from B")))
		backendC.EXPECT().Put(gomock.Any(), digestB, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(maximumMessageSizeBytes)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world from B"), data)
				return nil
			})
		backendC.EXPECT().Get(ctx, digestC).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world from C")))
		backendA.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), digestC, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, digest.EmptySet, missing)
		require.Equal(
			t,
			fmt.Errorf("Can't replicate block %s to Backend A: rpc error: code = Internal desc = Server on fire, Can't replicate block %s to Backend B: rpc error: code = Internal desc = Server on fire", digestC, digestC),
			err)
	})
}

/*
		// Use this code to convert a proto to a byte slice
		b := buffer.NewProtoBufferFromProto(&testInvalidatedConflictingActionResultMessage2, buffer.UserProvided)
		n, err := b.GetSizeBytes()
		require.NoError(t, err)
		s, err := b.ToByteSlice(int(n))
		require.NoError(t, err)
                fmt.Printf("testInvalidatedConflictingActionResultMessage2 size = %d, contents =\n", n)
		nb := 8
		i := 0
		for nb < int(n) {
			fmt.Printf("0x%.2x, 0x%.2x, 0x%.2x, 0x%.2x, 0x%.2x, 0x%.2x, 0x%.2x, 0x%.2x,\n", s[i], s[i+1], s[i+2], s[i+3], s[i+4], s[i+5], s[i+6], s[i+7])
			i += 8
			n -= 8
		}
		for n > 0 {
			fmt.Printf("0x%.2x, ", s[i])
			i++
			n--
		}
		fmt.Printf("\n")
		b.Discard()
*/
