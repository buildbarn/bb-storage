package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestAddMetadataUnaryClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	interceptor := bb_grpc.NewAddMetadataUnaryClientInterceptor([]string{"header", "value"})
	invoker := mock.NewMockUnaryInvoker(ctrl)
	req := &empty.Empty{}
	resp := &empty.Empty{}

	t.Run("AddHeader", func(t *testing.T) {
		// Outgoing request metadata should be extended
		// with pair ("header", "value").
		invoker.EXPECT().Call(gomock.Any(), "SomeMethod", req, resp, nil).DoAndReturn(
			func(ctx context.Context, method string, req interface{}, resp interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				md, ok := metadata.FromOutgoingContext(ctx)
				require.True(t, ok)
				require.Equal(
					t,
					metadata.New(map[string]string{
						"header": "value",
					}),
					md)
				return nil
			})

		require.NoError(t, interceptor(ctx, "SomeMethod", req, resp, nil, invoker.Call))
	})

}

func TestAddMetadataStreamClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	interceptor := bb_grpc.NewAddMetadataStreamClientInterceptor([]string{"header", "value"})
	streamDesc := grpc.StreamDesc{StreamName: "SomeMethod"}
	streamer := mock.NewMockStreamer(ctrl)
	clientStream := mock.NewMockClientStream(ctrl)

	t.Run("AddHeader", func(t *testing.T) {
		// Outgoing request metadata should be extended
		// with pair ("header", "value").
		streamer.EXPECT().Call(gomock.Any(), &streamDesc, nil, "SomeMethod").DoAndReturn(
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				md, ok := metadata.FromOutgoingContext(ctx)
				require.True(t, ok)
				require.Equal(
					t,
					metadata.New(map[string]string{
						"header": "value",
					}),
					md)
				return clientStream, nil
			})

		actualClientStream, err := interceptor(ctx, &streamDesc, nil, "SomeMethod", streamer.Call)
		require.NoError(t, err)
		require.Equal(t, clientStream, actualClientStream)
	})
}
