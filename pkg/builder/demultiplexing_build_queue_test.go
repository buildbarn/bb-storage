package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/mock"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDemultiplexingBuildQueueBadInstanceName(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	buildQueueGetter := mock.NewMockBuildQueueGetter(ctrl)
	demultiplexingBuildQueue := builder.NewDemultiplexingBuildQueue(buildQueueGetter.Call)

	_, err := demultiplexingBuildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
		InstanceName: "Hello|World",
	})
	require.Equal(t, status.Error(codes.InvalidArgument, "Instance name cannot contain a pipe character"), err)

	executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
	err = demultiplexingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
		InstanceName: "Hello|World",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 0,
		},
	}, executeServer)
	require.Equal(t, status.Error(codes.InvalidArgument, "Instance name cannot contain a pipe character"), err)

	waitExecutionServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
	err = demultiplexingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
		Name: "This is an operation name that doesn't contain a pipe, meaning we can't demultiplex",
	}, waitExecutionServer)
	require.Equal(t, status.Error(codes.InvalidArgument, "Unable to extract instance from operation name"), err)
}

func TestDemultiplexingBuildQueueFailedToGetBackend(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()
	buildQueueGetter := mock.NewMockBuildQueueGetter(ctrl)
	demultiplexingBuildQueue := builder.NewDemultiplexingBuildQueue(buildQueueGetter.Call)

	buildQueueGetter.EXPECT().Call("Nonexistent backend").Return(nil, status.Error(codes.NotFound, "Backend not found"))
	_, err := demultiplexingBuildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
		InstanceName: "Nonexistent backend",
	})
	require.Equal(t, status.Error(codes.NotFound, "Failed to obtain backend for instance \"Nonexistent backend\": Backend not found"), err)

	buildQueueGetter.EXPECT().Call("Nonexistent backend").Return(nil, status.Error(codes.NotFound, "Backend not found"))
	executeServer := mock.NewMockExecution_ExecuteServer(ctrl)
	err = demultiplexingBuildQueue.Execute(&remoteexecution.ExecuteRequest{
		InstanceName: "Nonexistent backend",
		ActionDigest: &remoteexecution.Digest{
			Hash:      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			SizeBytes: 0,
		},
	}, executeServer)
	require.Equal(t, status.Error(codes.NotFound, "Failed to obtain backend for instance \"Nonexistent backend\": Backend not found"), err)

	buildQueueGetter.EXPECT().Call("Nonexistent backend").Return(nil, status.Error(codes.NotFound, "Backend not found"))
	waitExecutionServer := mock.NewMockExecution_WaitExecutionServer(ctrl)
	err = demultiplexingBuildQueue.WaitExecution(&remoteexecution.WaitExecutionRequest{
		Name: "Nonexistent backend|df4ab561-4e81-48c7-a387-edc7d899a76f",
	}, waitExecutionServer)
	require.Equal(t, status.Error(codes.NotFound, "Failed to obtain backend for instance \"Nonexistent backend\": Backend not found"), err)
}

// TODO(edsch): Improve coverage.
