package blobstore_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActionResultExpiringBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	blobAccess := blobstore.NewActionResultExpiringBlobAccess(baseBlobAccess, clock, 10000, 28*24*time.Hour, 28*24*time.Hour)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "09b6c5db18b5e8db9ca5400c5ced1a0f", 123)

	t.Run("BackendFailure", func(t *testing.T) {
		// Failures from the backend should be propagated in
		// literal form.
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server not reachable")))

		_, err := blobAccess.Get(ctx, blobDigest).ToProto(&remoteexecution.ActionResult{}, 10000)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server not reachable"), err)
	})

	t.Run("NoMetadata", func(t *testing.T) {
		// If metadata is absent, we don't know when the action
		// was built. Simply pass through the results.
		desiredActionResult := &remoteexecution.ActionResult{
			StderrRaw: []byte("Internal compiler error!"),
			ExitCode:  1,
		}
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewProtoBufferFromProto(desiredActionResult, buffer.UserProvided))

		actualActionResult, err := blobAccess.Get(ctx, blobDigest).ToProto(&remoteexecution.ActionResult{}, 10000)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, desiredActionResult, actualActionResult)
	})

	t.Run("EmptyMetadata", func(t *testing.T) {
		desiredActionResult := &remoteexecution.ActionResult{
			StderrRaw:         []byte("Internal compiler error!"),
			ExitCode:          1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{},
		}
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewProtoBufferFromProto(desiredActionResult, buffer.UserProvided))

		actualActionResult, err := blobAccess.Get(ctx, blobDigest).ToProto(&remoteexecution.ActionResult{}, 10000)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, desiredActionResult, actualActionResult)
	})

	t.Run("StillValid", func(t *testing.T) {
		// Request an entry right before it's about to expire.
		desiredActionResult := &remoteexecution.ActionResult{
			StderrRaw: []byte("Internal compiler error!"),
			ExitCode:  1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 1641325786},
			},
		}
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewProtoBufferFromProto(desiredActionResult, buffer.UserProvided))
		clock.EXPECT().Now().Return(time.Unix(1644187855, 0))

		actualActionResult, err := blobAccess.Get(ctx, blobDigest).ToProto(&remoteexecution.ActionResult{}, 10000)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, desiredActionResult, actualActionResult)
	})

	t.Run("Expired", func(t *testing.T) {
		// Request an entry right after it expired.
		desiredActionResult := &remoteexecution.ActionResult{
			StderrRaw: []byte("Internal compiler error!"),
			ExitCode:  1,
			ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
				WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 1641325786},
			},
		}
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewProtoBufferFromProto(desiredActionResult, buffer.UserProvided))
		clock.EXPECT().Now().Return(time.Unix(1644187856, 0))

		_, err := blobAccess.Get(ctx, blobDigest).ToProto(&remoteexecution.ActionResult{}, 10000)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Action result expired at 2022-02-06T22:50:55Z"), err)
	})
}
