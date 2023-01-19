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

	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestActionResultTimestampInjectingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	clock := mock.NewMockClock(ctrl)
	blobAccess := blobstore.NewActionResultTimestampInjectingBlobAccess(baseBlobAccess, clock, 1000)
	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "d3b7ed68c99422eaa8ab8184949cba84dd46ddb1b7cf8c777547866d54ebb081", 123)

	t.Run("NoMetadata", func(t *testing.T) {
		// If the ActionResult contains no execution metadata,
		// it should be added, only containing a worker
		// completed timestamp.
		clock.EXPECT().Now().Return(time.Unix(123, 456))
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						WorkerCompletedTimestamp: &timestamppb.Timestamp{
							Seconds: 123,
							Nanos:   456,
						},
					},
					ExitCode: 123,
				}, actionResult)
				return nil
			})

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				blobDigest,
				buffer.NewProtoBufferFromProto(
					&remoteexecution.ActionResult{
						ExitCode: 123,
					},
					buffer.UserProvided)))
	})

	t.Run("MetadataWithoutWorkerCompletedTimestamp", func(t *testing.T) {
		// If the ActionResult contains execution metadata, but
		// doesn't have worker_completed_timestamp set, it
		// should be added to the existing metadata.
		clock.EXPECT().Now().Return(time.Unix(1400, 0))
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						WorkerStartTimestamp:     &timestamppb.Timestamp{Seconds: 1300},
						WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 1400},
					},
				}, actionResult)
				return nil
			})

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				blobDigest,
				buffer.NewProtoBufferFromProto(
					&remoteexecution.ActionResult{
						ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
							WorkerStartTimestamp: &timestamppb.Timestamp{Seconds: 1300},
						},
					},
					buffer.UserProvided)))
	})

	t.Run("MetadataWithWorkerCompletedTimestamp", func(t *testing.T) {
		// If the ActionResult already has
		// worker_completed_timestamp set, the request should be
		// forwared in literal form.
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				actionResult, err := b.ToProto(&remoteexecution.ActionResult{}, 1000)
				require.NoError(t, err)
				testutil.RequireEqualProto(t, &remoteexecution.ActionResult{
					ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
						WorkerStartTimestamp:     &timestamppb.Timestamp{Seconds: 2000},
						WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 2100},
					},
				}, actionResult)
				return nil
			})

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				blobDigest,
				buffer.NewProtoBufferFromProto(
					&remoteexecution.ActionResult{
						ExecutionMetadata: &remoteexecution.ExecutedActionMetadata{
							WorkerStartTimestamp:     &timestamppb.Timestamp{Seconds: 2000},
							WorkerCompletedTimestamp: &timestamppb.Timestamp{Seconds: 2100},
						},
					},
					buffer.UserProvided)))
	})
}
