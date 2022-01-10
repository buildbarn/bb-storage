package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestAuthenticatingUnaryInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	authenticator := mock.NewMockAuthenticator(ctrl)
	authenticator.EXPECT().Authenticate(ctx).Return("You're totally who you say you are", nil)

	interceptor := bb_grpc.NewAuthenticatingUnaryInterceptor(authenticator)
	handler := mock.NewMockUnaryHandler(ctrl)
	req := &emptypb.Empty{}
	resp := &emptypb.Empty{}

	t.Run("ReturnsModifiedCtx", func(t *testing.T) {
		handler.EXPECT().Call(gomock.Any(), req).DoAndReturn(
			func(ctx context.Context, req interface{}) (interface{}, error) {
				require.Equal(t, "You're totally who you say you are", ctx.Value(auth.AuthenticationMetadata{}))
				return resp, nil
			})

		gotResp, err := interceptor(ctx, req, nil, handler.Call)
		require.NoError(t, err)
		require.Equal(t, resp, gotResp)
	})
}

func TestAuthenticatingStreamInterceptor(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	authenticator := mock.NewMockAuthenticator(ctrl)
	authenticator.EXPECT().Authenticate(ctx).Return("You're totally who you say you are", nil)

	interceptor := bb_grpc.NewAuthenticatingStreamInterceptor(authenticator)
	handler := mock.NewMockStreamHandler(ctrl)

	serverStream := mock.NewMockServerStream(ctrl)
	serverStream.EXPECT().Context().Return(ctx).AnyTimes()

	t.Run("ReturnsModifiedCtx", func(t *testing.T) {
		handler.EXPECT().Call(gomock.Any(), gomock.Any()).DoAndReturn(
			func(srv interface{}, stream grpc.ServerStream) error {
				require.Equal(t, "You're totally who you say you are", stream.Context().Value(auth.AuthenticationMetadata{}))
				return nil
			})

		require.NoError(t, interceptor(nil, serverStream, nil, handler.Call))
	})
}
