package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"

	"go.uber.org/mock/gomock"
)

func TestDemultiplexingBuildQueueGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	buildQueueGetter := mock.NewMockDemultiplexedBuildQueueGetter(ctrl)
	demultiplexingBuildQueue := builder.NewDemultiplexingBuildQueue(buildQueueGetter.Call)

	t.Run("NonexistentInstanceName", func(t *testing.T) {
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("Nonexistent backend"))).Return(
			nil,
			digest.EmptyInstanceName,
			digest.EmptyInstanceName,
			status.Error(codes.NotFound, "Backend not found"))

		_, err := demultiplexingBuildQueue.GetCapabilities(ctx, util.Must(digest.NewInstanceName("Nonexistent backend")))
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to obtain backend for instance name \"Nonexistent backend\": Backend not found"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		buildQueue := mock.NewMockBuildQueue(ctrl)
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("ubuntu1804"))).Return(
			buildQueue,
			digest.EmptyInstanceName,
			util.Must(digest.NewInstanceName("rhel7")),
			nil)
		buildQueue.EXPECT().GetCapabilities(ctx, util.Must(digest.NewInstanceName("rhel7"))).
			Return(nil, status.Error(codes.Unavailable, "Server not reachable"))

		_, err := demultiplexingBuildQueue.GetCapabilities(ctx, util.Must(digest.NewInstanceName("ubuntu1804")))
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server not reachable"), err)
	})

	t.Run("Success", func(t *testing.T) {
		buildQueue := mock.NewMockBuildQueue(ctrl)
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("ubuntu1804"))).Return(
			buildQueue,
			digest.EmptyInstanceName,
			util.Must(digest.NewInstanceName("rhel7")),
			nil)
		buildQueue.EXPECT().GetCapabilities(ctx, util.Must(digest.NewInstanceName("rhel7"))).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: digest.SupportedDigestFunctions,
				},
			}, nil)

		response, err := demultiplexingBuildQueue.GetCapabilities(ctx, util.Must(digest.NewInstanceName("ubuntu1804")))
		require.NoError(t, err)
		require.Equal(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
			},
		}, response)
	})
}

func TestDemultiplexingBuildQueueExecute(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	buildQueueGetter := mock.NewMockDemultiplexedBuildQueueGetter(ctrl)
	demultiplexingBuildQueue := builder.NewDemultiplexingBuildQueue(buildQueueGetter.Call)

	t.Run("InvalidInstanceName", func(t *testing.T) {
		executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
		executeServer.EXPECT().Context().Return(ctx).AnyTimes()

		err := demultiplexingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "hello/blobs/world",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid instance name \"hello/blobs/world\": Instance name contains reserved keyword \"blobs\""), err)
	})

	t.Run("NonexistentInstanceName", func(t *testing.T) {
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("Nonexistent backend"))).Return(
			nil,
			digest.EmptyInstanceName,
			digest.EmptyInstanceName,
			status.Error(codes.NotFound, "Backend not found"))
		executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
		executeServer.EXPECT().Context().Return(ctx).AnyTimes()

		err := demultiplexingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "Nonexistent backend",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to obtain backend for instance name \"Nonexistent backend\": Backend not found"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		buildQueue := mock.NewMockBuildQueue(ctrl)
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("ubuntu1804"))).Return(
			buildQueue,
			digest.EmptyInstanceName,
			util.Must(digest.NewInstanceName("rhel7")),
			nil)
		buildQueue.EXPECT().Execute(testutil.EqProto(t, &remoteexecution.ExecuteRequest{
			InstanceName: "rhel7",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}), gomock.Any()).Return(status.Error(codes.Unavailable, "Server not reachable"))
		executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
		executeServer.EXPECT().Context().Return(ctx).AnyTimes()

		err := demultiplexingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "ubuntu1804",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server not reachable"), err)
	})

	t.Run("Success", func(t *testing.T) {
		buildQueue := mock.NewMockBuildQueue(ctrl)
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("foo/ubuntu1804"))).Return(
			buildQueue,
			util.Must(digest.NewInstanceName("foo")),
			util.Must(digest.NewInstanceName("rhel7")),
			nil)
		buildQueue.EXPECT().Execute(testutil.EqProto(t, &remoteexecution.ExecuteRequest{
			InstanceName: "rhel7",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}), gomock.Any()).DoAndReturn(
			func(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
				require.NoError(t, out.Send(&longrunningpb.Operation{
					Name: "fd6ee599-dee5-4390-a221-2bd34cd8ff53",
					Done: true,
				}))
				return nil
			})
		executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
		executeServer.EXPECT().Context().Return(ctx).AnyTimes()
		executeServer.EXPECT().Send(testutil.EqProto(t, &longrunningpb.Operation{
			// We should return the operation name prefixed
			// with the identifying part of the instance
			// name, so that WaitExecution() can forward
			// calls to the right backend based on the
			// operation name.
			Name: "foo/operations/fd6ee599-dee5-4390-a221-2bd34cd8ff53",
			Done: true,
		}))

		err := demultiplexingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "foo/ubuntu1804",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		require.NoError(t, err)
	})
}

func TestDemultiplexingBuildQueueWaitExecution(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	buildQueueGetter := mock.NewMockDemultiplexedBuildQueueGetter(ctrl)
	demultiplexingBuildQueue := builder.NewDemultiplexingBuildQueue(buildQueueGetter.Call)

	t.Run("InvalidInstanceName", func(t *testing.T) {
		waitExecutionServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
		waitExecutionServer.EXPECT().Context().Return(ctx).AnyTimes()

		err := demultiplexingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "This is an operation name that doesn't contain slash-operations-slash, meaning we can't demultiplex",
		}, waitExecutionServer)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Unable to extract instance name from operation name"), err)
	})

	t.Run("NonexistentInstanceName", func(t *testing.T) {
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("Nonexistent backend"))).Return(
			nil,
			digest.EmptyInstanceName,
			digest.EmptyInstanceName,
			status.Error(codes.NotFound, "Backend not found"))
		waitExecutionServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
		waitExecutionServer.EXPECT().Context().Return(ctx).AnyTimes()

		err := demultiplexingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "Nonexistent backend/operations/df4ab561-4e81-48c7-a387-edc7d899a76f",
		}, waitExecutionServer)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to obtain backend for instance name \"Nonexistent backend\": Backend not found"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		buildQueue := mock.NewMockBuildQueue(ctrl)
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("ubuntu1804"))).Return(
			buildQueue,
			util.Must(digest.NewInstanceName("ubuntu1804")),
			util.Must(digest.NewInstanceName("rhel7")),
			nil)
		buildQueue.EXPECT().WaitExecution(testutil.EqProto(t, &remoteexecution.WaitExecutionRequest{
			Name: "df4ab561-4e81-48c7-a387-edc7d899a76f",
		}), gomock.Any()).Return(status.Error(codes.Unavailable, "Server not reachable"))
		waitExecutionServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
		waitExecutionServer.EXPECT().Context().Return(ctx).AnyTimes()

		err := demultiplexingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "ubuntu1804/operations/df4ab561-4e81-48c7-a387-edc7d899a76f",
		}, waitExecutionServer)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server not reachable"), err)
	})

	t.Run("Success", func(t *testing.T) {
		buildQueue := mock.NewMockBuildQueue(ctrl)
		buildQueueGetter.EXPECT().Call(ctx, util.Must(digest.NewInstanceName("ubuntu1804"))).Return(
			buildQueue,
			util.Must(digest.NewInstanceName("ubuntu1804")),
			util.Must(digest.NewInstanceName("rhel7")),
			nil)
		buildQueue.EXPECT().WaitExecution(testutil.EqProto(t, &remoteexecution.WaitExecutionRequest{
			Name: "df4ab561-4e81-48c7-a387-edc7d899a76f",
		}), gomock.Any()).DoAndReturn(
			func(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
				require.NoError(t, out.Send(&longrunningpb.Operation{
					Name: "df4ab561-4e81-48c7-a387-edc7d899a76f",
					Done: true,
				}))
				return nil
			})
		waitExecutionServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
		waitExecutionServer.EXPECT().Context().Return(ctx).AnyTimes()
		waitExecutionServer.EXPECT().Send(testutil.EqProto(t, &longrunningpb.Operation{
			// We should return the operation name prefixed
			// with the identifying part of the instance
			// name, so that WaitExecution() can forward
			// calls to the right backend based on the
			// operation name.
			Name: "ubuntu1804/operations/df4ab561-4e81-48c7-a387-edc7d899a76f",
			Done: true,
		}))

		err := demultiplexingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "ubuntu1804/operations/df4ab561-4e81-48c7-a387-edc7d899a76f",
		}, waitExecutionServer)
		require.NoError(t, err)
	})
}
