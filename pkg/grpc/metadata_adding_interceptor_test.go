package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"

	"go.uber.org/mock/gomock"
)

func TestMetadataAddingUnaryClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	var headerValues bb_grpc.MetadataHeaderValues
	headerValues.Add("header", []string{"value"})
	interceptor := bb_grpc.NewMetadataAddingUnaryClientInterceptor(headerValues)
	invoker := mock.NewMockUnaryInvoker(ctrl)
	req := &emptypb.Empty{}
	resp := &emptypb.Empty{}

	t.Run("AddHeader", func(t *testing.T) {
		// Outgoing request metadata should be extended
		// with pair ("header", "value").
		invoker.EXPECT().Call(gomock.Any(), "SomeMethod", req, resp, nil).DoAndReturn(
			func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
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

func TestMetadataAddingStreamClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	var headerValues bb_grpc.MetadataHeaderValues
	headerValues.Add("header", []string{"value"})
	interceptor := bb_grpc.NewMetadataAddingStreamClientInterceptor(headerValues)
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
