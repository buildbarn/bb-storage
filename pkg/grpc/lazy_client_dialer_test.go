package grpc_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestLazyClientDialer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseClientDialer := mock.NewMockClientDialer(ctrl)
	clientDialer := bb_grpc.NewLazyClientDialer(baseClientDialer.Call)

	t.Run("DoNothing", func(t *testing.T) {
		// No actual client connection should be created if no
		// RPCs are performed.
		_, err := clientDialer(ctx, "hello", grpc.WithBlock())
		require.NoError(t, err)
	})

	t.Run("FailureAndSuccess", func(t *testing.T) {
		// The first time an RPC is called, a client connection
		// should be created. If this fails, the error should be
		// propagated.
		baseClientDialer.EXPECT().Call(ctx, "hello").
			Return(nil, status.Error(codes.Internal, "Some error occurred"))

		clientConnection, err := clientDialer(ctx, "hello")
		require.NoError(t, err)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Failed to create client connection: Some error occurred"),
			clientConnection.Invoke(ctx, "method", nil, nil))

		// The next time an RPC is called, we should retry
		// creating the client connection.
		baseClientConnection := mock.NewMockClientConnInterface(ctrl)
		baseClientDialer.EXPECT().Call(ctx, "hello").
			Return(baseClientConnection, nil)
		baseClientConnection.EXPECT().Invoke(ctx, "method", nil, nil)

		require.NoError(
			t,
			clientConnection.Invoke(ctx, "method", nil, nil))

		// Future calls should no longer create client
		// connections, as the previous one can be reused.
		baseClientConnection.EXPECT().Invoke(ctx, "method", nil, nil)

		require.NoError(
			t,
			clientConnection.Invoke(ctx, "method", nil, nil))
	})
}
