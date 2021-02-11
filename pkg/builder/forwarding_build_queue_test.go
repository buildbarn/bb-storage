package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestForwardingBuildQueue(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	buildQueue := builder.NewForwardingBuildQueue(client)

	t.Run("SendFailure", func(t *testing.T) {
		// Simulate the case where we get a response from the
		// scheduler, but fail to forward it back to the client.
		// The context should be cancelled. Any trailing
		// messages should be read and discarded.
		executeRequest := remoteexecution.ExecuteRequest{
			InstanceName: "my-scheduler",
			ActionDigest: &remoteexecution.Digest{
				Hash:      "4c8e1b4bccdba0bb9572556988b703bb",
				SizeBytes: 241,
			},
		}
		operation := longrunning.Operation{
			Name: "9050db2b-8055-4ad1-ba94-7b8068ff4b73",
		}

		// Send the initial ExecuteRequest.
		out := mock.NewMockExecution_ExecuteServer(ctrl)
		out.EXPECT().Context().Return(ctx)
		clientStream := mock.NewMockClientStream(ctrl)
		var ctxClient context.Context
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/build.bazel.remote.execution.v2.Execution/Execute").
			DoAndReturn(func(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				ctxClient = ctx
				return clientStream, nil
			})
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &executeRequest))
		clientStream.EXPECT().CloseSend()

		// Return an Operation that cannot be forwarded.
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			proto.Merge(m.(proto.Message), &operation)
			return nil
		})
		out.EXPECT().Send(testutil.EqProto(t, &operation)).
			Return(status.Error(codes.Unavailable, "Client has closed connection"))

		// Trailing messages.
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			proto.Merge(m.(proto.Message), &operation)
			return nil
		})
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			<-ctxClient.Done()
			require.Equal(t, context.Canceled, ctxClient.Err())
			return status.Error(codes.Canceled, "Request has been canceled")
		})

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Client has closed connection"),
			buildQueue.Execute(&executeRequest, out))
	})
}
