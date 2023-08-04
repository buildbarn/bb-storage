package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

func TestAuthorizingBuildQueueGetExecutionCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	buildQueue := mock.NewMockBuildQueue(ctrl)
	authorizer := mock.NewMockAuthorizer(ctrl)
	authorizingBuildQueue := builder.NewAuthorizingBuildQueue(buildQueue, authorizer)

	instanceName := digest.MustNewInstanceName("hello/world")

	t.Run("NoExecutionCapabilities", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{instanceName}).Return([]error{nil})
		buildQueue.EXPECT().GetCapabilities(ctx, instanceName).Return(&remoteexecution.ServerCapabilities{}, nil)

		caps, err := authorizingBuildQueue.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		require.Nil(t, caps.ExecutionCapabilities)
	})

	t.Run("NotAuthorized", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{instanceName}).Return([]error{status.Error(codes.PermissionDenied, "Permission denied")})

		_, err := authorizingBuildQueue.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: Permission denied"), err)
	})

	t.Run("AuthError", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{instanceName}).Return([]error{status.Error(codes.Internal, "Something went wrong")})

		_, err := authorizingBuildQueue.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Authorization: Something went wrong"), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{instanceName}).Return([]error{nil})
		wantErr := status.Error(codes.Internal, "Something went wrong")
		buildQueue.EXPECT().GetCapabilities(ctx, instanceName).Return(nil, wantErr)

		_, err := authorizingBuildQueue.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, wantErr, err)
	})

	t.Run("Success", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{instanceName}).Return([]error{nil})
		buildQueue.EXPECT().GetCapabilities(ctx, instanceName).Return(&remoteexecution.ServerCapabilities{
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, nil)

		caps, err := authorizingBuildQueue.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		require.True(t, caps.ExecutionCapabilities.GetExecEnabled())
	})
}

func TestAuthorizingBuildQueueExecute(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	buildQueue := mock.NewMockBuildQueue(ctrl)
	executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
	executeServer.EXPECT().Context().Return(ctx).AnyTimes()
	authorizer := mock.NewMockAuthorizer(ctrl)
	authorizingBuildQueue := builder.NewAuthorizingBuildQueue(buildQueue, authorizer)

	t.Run("InvalidInstanceName", func(t *testing.T) {
		err := authorizingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "hello/blobs/world",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid instance name \"hello/blobs/world\": Instance name contains reserved keyword \"blobs\""), err)
	})

	t.Run("Denied", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{digest.MustNewInstanceName("hello/world")}).Return([]error{status.Error(codes.PermissionDenied, "Permission denied")})
		err := authorizingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "hello/world",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Failed to authorize to Execute() against instance name \"hello/world\": Permission denied"), err)
	})

	t.Run("Allowed", func(t *testing.T) {
		authorizer.EXPECT().Authorize(ctx, []digest.InstanceName{digest.MustNewInstanceName("hello/world")}).Return([]error{nil})
		buildQueue.EXPECT().Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "hello/world",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, gomock.Any()).DoAndReturn(
			func(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
				require.NoError(t, out.Send(&longrunningpb.Operation{
					Name: "fd6ee599-dee5-4390-a221-2bd34cd8ff53",
					Done: true,
				}))
				return nil
			})
		executeServer.EXPECT().Send(&longrunningpb.Operation{
			Name: "fd6ee599-dee5-4390-a221-2bd34cd8ff53",
			Done: true,
		})

		err := authorizingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
			InstanceName: "hello/world",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
				SizeBytes: 0,
			},
		}, executeServer)
		require.NoError(t, err)
	})
}

func TestAuthorizingBuildQueueWaitExecution(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	buildQueue := mock.NewMockBuildQueue(ctrl)
	executeServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
	executeServer.EXPECT().Context().Return(ctx).AnyTimes()
	authorizer := mock.NewMockAuthorizer(ctrl)
	authorizingBuildQueue := builder.NewAuthorizingBuildQueue(buildQueue, authorizer)

	t.Run("Success", func(t *testing.T) {
		buildQueue.EXPECT().WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "3fc21c92-5db0-42e5-b657-ddf6f937b348",
		}, gomock.Any()).DoAndReturn(
			func(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
				require.NoError(t, out.Send(&longrunningpb.Operation{
					Name: "fd6ee599-dee5-4390-a221-2bd34cd8ff53",
					Done: true,
				}))
				return nil
			})
		executeServer.EXPECT().Send(&longrunningpb.Operation{
			Name: "fd6ee599-dee5-4390-a221-2bd34cd8ff53",
			Done: true,
		})

		require.NoError(t, authorizingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "3fc21c92-5db0-42e5-b657-ddf6f937b348",
		}, executeServer))
	})

	t.Run("Failure", func(t *testing.T) {
		buildQueue.EXPECT().WaitExecution(&remoteexecution.WaitExecutionRequest{
			Name: "3fc21c92-5db0-42e5-b657-ddf6f937b348",
		}, gomock.Any()).Return(status.Error(codes.Unavailable, "Server not available"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Server not available"),
			authorizingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
				Name: "3fc21c92-5db0-42e5-b657-ddf6f937b348",
			}, executeServer))
	})
}
