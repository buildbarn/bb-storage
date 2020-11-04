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

func TestMetadataForwardingUnaryClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	interceptor := bb_grpc.NewMetadataForwardingUnaryClientInterceptor([]string{"authorization"})
	invoker := mock.NewMockUnaryInvoker(ctrl)
	req := &empty.Empty{}
	resp := &empty.Empty{}

	t.Run("NoIncomingMetadata", func(t *testing.T) {
		// If the request contains no incoming request metadata,
		// no outgoing request metadata should be added.
		invoker.EXPECT().Call(ctx, "SomeMethod", req, resp, nil).Return(nil)

		require.NoError(t, interceptor(ctx, "SomeMethod", req, resp, nil, invoker.Call))
	})

	t.Run("NoAuthorizationHeader", func(t *testing.T) {
		// If the incoming request metadata does not contain any
		// matching header, no outgoing request metadata should
		// be added.
		ctxWithMetadata := metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{
				"foo": "bar",
			}))
		invoker.EXPECT().Call(ctxWithMetadata, "SomeMethod", req, resp, nil).Return(nil)

		require.NoError(t, interceptor(
			metadata.NewIncomingContext(
				ctx,
				metadata.New(map[string]string{
					"foo": "bar",
				})),
			"SomeMethod", req, resp, nil, invoker.Call))
	})

	t.Run("Success", func(t *testing.T) {
		// If the incoming request metadata contains a matching
		// header, the outgoing request metadata should be
		// extended. Only matching headers should get copied.
		invoker.EXPECT().Call(gomock.Any(), "SomeMethod", req, resp, nil).DoAndReturn(
			func(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
				md, ok := metadata.FromOutgoingContext(ctx)
				require.True(t, ok)
				require.Equal(
					t,
					metadata.New(map[string]string{
						"authorization": "Bearer token123",
					}),
					md)
				return nil
			})

		require.NoError(t, interceptor(
			metadata.NewIncomingContext(
				ctx,
				metadata.New(map[string]string{
					"authorization": "Bearer token123",
					"foo":           "bar",
				})),
			"SomeMethod", req, resp, nil, invoker.Call))
	})
}

func TestMetadataForwardingStreamClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	interceptor := bb_grpc.NewMetadataForwardingStreamClientInterceptor([]string{"authorization"})
	streamDesc := grpc.StreamDesc{StreamName: "SomeMethod"}
	streamer := mock.NewMockStreamer(ctrl)
	clientStream := mock.NewMockClientStream(ctrl)

	t.Run("NoIncomingMetadata", func(t *testing.T) {
		// If the request contains no incoming request metadata,
		// no outgoing request metadata should be added.
		streamer.EXPECT().Call(ctx, &streamDesc, nil, "SomeMethod").Return(clientStream, nil)

		actualClientStream, err := interceptor(ctx, &streamDesc, nil, "SomeMethod", streamer.Call)
		require.NoError(t, err)
		require.Equal(t, clientStream, actualClientStream)
	})

	t.Run("NoAuthorizationHeader", func(t *testing.T) {
		// If the incoming request metadata does not contain any
		// matching header, no outgoing request metadata should
		// be added.
		ctxWithMetadata := metadata.NewIncomingContext(
			ctx,
			metadata.New(map[string]string{
				"foo": "bar",
			}))
		streamer.EXPECT().Call(ctxWithMetadata, &streamDesc, nil, "SomeMethod").Return(clientStream, nil)

		actualClientStream, err := interceptor(
			metadata.NewIncomingContext(
				ctx,
				metadata.New(map[string]string{
					"foo": "bar",
				})),
			&streamDesc, nil, "SomeMethod", streamer.Call)
		require.NoError(t, err)
		require.Equal(t, clientStream, actualClientStream)
	})

	t.Run("Success", func(t *testing.T) {
		// If the incoming request metadata contains a matching
		// header, the outgoing request metadata should be
		// extended. Only matching headers should get copied.
		streamer.EXPECT().Call(gomock.Any(), &streamDesc, nil, "SomeMethod").DoAndReturn(
			func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
				md, ok := metadata.FromOutgoingContext(ctx)
				require.True(t, ok)
				require.Equal(
					t,
					metadata.New(map[string]string{
						"authorization": "Bearer token123",
					}),
					md)
				return clientStream, nil
			})

		actualClientStream, err := interceptor(
			metadata.NewIncomingContext(
				ctx,
				metadata.New(map[string]string{
					"authorization": "Bearer token123",
					"foo":           "bar",
				})),
			&streamDesc, nil, "SomeMethod", streamer.Call)
		require.NoError(t, err)
		require.Equal(t, clientStream, actualClientStream)
	})
}
