package grpc_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	"go.uber.org/mock/gomock"
)

func TestMetadataExtractingAndForwardingUnaryClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	modifiedCtx := auth.NewContextWithAuthenticationMetadata(ctx, util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Private: structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"header": structpb.NewStringValue("value"),
			},
		}),
	})))
	invoker := mock.NewMockUnaryInvoker(ctrl)
	req := &emptypb.Empty{}
	resp := &emptypb.Empty{}

	t.Run("AddHeader", func(t *testing.T) {
		interceptor := bb_grpc.NewMetadataExtractingAndForwardingUnaryClientInterceptor(func(ctx context.Context) (bb_grpc.MetadataHeaderValues, error) {
			kv := auth.AuthenticationMetadataFromContext(ctx).GetRaw()["private"]
			var headers bb_grpc.MetadataHeaderValues
			for k, v := range kv.(map[string]any) {
				headers = append(headers, k, v.(string))
			}
			return headers, nil
		})
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

		require.NoError(t, interceptor(modifiedCtx, "SomeMethod", req, resp, nil, invoker.Call))
	})

	t.Run("Error", func(t *testing.T) {
		interceptor := bb_grpc.NewMetadataExtractingAndForwardingUnaryClientInterceptor(func(ctx context.Context) (bb_grpc.MetadataHeaderValues, error) {
			return nil, status.Error(codes.NotFound, "Error loading metadata")
		})
		err := interceptor(modifiedCtx, "SomeMethod", req, resp, nil, invoker.Call)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to extract metadata: Error loading metadata"), err)
	})
}

func TestMetadataExtractingAndForwardingStreamClientInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	modifiedCtx := auth.NewContextWithAuthenticationMetadata(ctx, util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
		Private: structpb.NewStructValue(&structpb.Struct{
			Fields: map[string]*structpb.Value{
				"header": structpb.NewStringValue("value"),
			},
		}),
	})))
	streamDesc := grpc.StreamDesc{StreamName: "SomeMethod"}
	streamer := mock.NewMockStreamer(ctrl)
	clientStream := mock.NewMockClientStream(ctrl)

	t.Run("AddHeader", func(t *testing.T) {
		interceptor := bb_grpc.NewMetadataExtractingAndForwardingStreamClientInterceptor(func(ctx context.Context) (bb_grpc.MetadataHeaderValues, error) {
			kv := auth.AuthenticationMetadataFromContext(ctx).GetRaw()["private"]
			var headers bb_grpc.MetadataHeaderValues
			for k, v := range kv.(map[string]any) {
				headers = append(headers, k, v.(string))
			}
			return headers, nil
		})
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

		actualClientStream, err := interceptor(modifiedCtx, &streamDesc, nil, "SomeMethod", streamer.Call)
		require.NoError(t, err)
		require.Equal(t, clientStream, actualClientStream)
	})

	t.Run("Error", func(t *testing.T) {
		interceptor := bb_grpc.NewMetadataExtractingAndForwardingStreamClientInterceptor(func(ctx context.Context) (bb_grpc.MetadataHeaderValues, error) {
			return nil, status.Error(codes.NotFound, "Error loading metadata")
		})
		_, err := interceptor(modifiedCtx, &streamDesc, nil, "SomeMethod", streamer.Call)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to extract metadata: Error loading metadata"), err)
	})
}
