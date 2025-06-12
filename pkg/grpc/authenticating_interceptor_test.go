package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	auth_pb "github.com/buildbarn/bb-storage/pkg/proto/auth"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/proto/otlp/common/v1"

	"go.uber.org/mock/gomock"
)

func TestAuthenticatingUnaryInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	authenticator := mock.NewMockGRPCAuthenticator(ctrl)

	interceptor := bb_grpc.NewAuthenticatingUnaryInterceptor(authenticator)
	handler := mock.NewMockUnaryHandler(ctrl)
	req := &emptypb.Empty{}
	resp := &emptypb.Empty{}

	t.Run("ReturnsModifiedCtx", func(t *testing.T) {
		authenticator.EXPECT().Authenticate(ctx).Return(util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("You're totally who you say you are"),
		})), nil)
		handler.EXPECT().Call(gomock.Any(), req).DoAndReturn(
			func(ctx context.Context, req interface{}) (interface{}, error) {
				require.Equal(t, map[string]any{
					"public": "You're totally who you say you are",
				}, auth.AuthenticationMetadataFromContext(ctx).GetRaw())
				return resp, nil
			})

		gotResp, err := interceptor(ctx, req, nil, handler.Call)
		require.NoError(t, err)
		require.Equal(t, resp, gotResp)
	})

	t.Run("InstallsSpanAttributes", func(t *testing.T) {
		span := mock.NewMockSpan(ctrl)
		ctxWithSpan := trace.ContextWithSpan(ctx, span)
		authenticator.EXPECT().Authenticate(ctxWithSpan).Return(util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			TracingAttributes: []*v1.KeyValue{
				{
					Key: "username",
					Value: &v1.AnyValue{
						Value: &v1.AnyValue_StringValue{
							StringValue: "john_doe",
						},
					},
				},
			},
		})), nil)
		span.EXPECT().SetAttributes(attribute.String("auth.username", "john_doe"))

		handler.EXPECT().Call(gomock.Any(), req).DoAndReturn(
			func(ctx context.Context, req interface{}) (interface{}, error) {
				return resp, nil
			})

		gotResp, err := interceptor(ctxWithSpan, req, nil, handler.Call)
		require.NoError(t, err)
		require.Equal(t, resp, gotResp)
	})
}

func TestAuthenticatingStreamInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	authenticator := mock.NewMockGRPCAuthenticator(ctrl)

	interceptor := bb_grpc.NewAuthenticatingStreamInterceptor(authenticator)
	handler := mock.NewMockStreamHandler(ctrl)

	t.Run("ReturnsModifiedCtx", func(t *testing.T) {
		serverStream := mock.NewMockServerStream(ctrl)
		serverStream.EXPECT().Context().Return(ctx).AnyTimes()
		authenticator.EXPECT().Authenticate(ctx).Return(util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			Public: structpb.NewStringValue("You're totally who you say you are"),
		})), nil)
		handler.EXPECT().Call(gomock.Any(), gomock.Any()).DoAndReturn(
			func(srv interface{}, stream grpc.ServerStream) error {
				require.Equal(t, map[string]any{
					"public": "You're totally who you say you are",
				}, auth.AuthenticationMetadataFromContext(stream.Context()).GetRaw())
				return nil
			})

		require.NoError(t, interceptor(nil, serverStream, nil, handler.Call))
	})

	t.Run("InstallsSpanAttributes", func(t *testing.T) {
		serverStream := mock.NewMockServerStream(ctrl)
		span := mock.NewMockSpan(ctrl)
		ctxWithSpan := trace.ContextWithSpan(ctx, span)
		serverStream.EXPECT().Context().Return(ctxWithSpan).AnyTimes()
		authenticator.EXPECT().Authenticate(ctxWithSpan).Return(util.Must(auth.NewAuthenticationMetadataFromProto(&auth_pb.AuthenticationMetadata{
			TracingAttributes: []*v1.KeyValue{
				{
					Key: "username",
					Value: &v1.AnyValue{
						Value: &v1.AnyValue_StringValue{
							StringValue: "john_doe",
						},
					},
				},
			},
		})), nil)
		handler.EXPECT().Call(gomock.Any(), gomock.Any()).DoAndReturn(
			func(srv interface{}, stream grpc.ServerStream) error {
				return nil
			})
		span.EXPECT().SetAttributes(attribute.String("auth.username", "john_doe"))

		require.NoError(t, interceptor(nil, serverStream, nil, handler.Call))
	})
}
