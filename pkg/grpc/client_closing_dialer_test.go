package grpc_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestClientClosingDialer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("CreationError", func(t *testing.T) {
		clientDialer := bb_grpc.NewClientClosingDialer(
			func(target string, opts ...grpc.DialOption) (bb_grpc.ClientConnCloser, error) {
				return nil, status.Error(codes.InvalidArgument, "Simply failed")
			},
		)
		program.RunLocal(ctx, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
			_, err := clientDialer(siblingsGroup, "example.com:1")
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Simply failed"), err)
			return nil
		})
	})

	t.Run("Success", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			clientConnCloser := mock.NewMockClientConnCloser(ctrl)
			clientDialer := bb_grpc.NewClientClosingDialer(
				func(target string, opts ...grpc.DialOption) (bb_grpc.ClientConnCloser, error) {
					return clientConnCloser, nil
				},
			)

			programCtx, cancel := context.WithCancel(ctx)
			closeHasBeenCalled := false
			err := program.RunLocal(programCtx, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				clientConn, err := clientDialer(dependenciesGroup, "example.com:1")
				require.Equal(t, clientConnCloser, clientConn)
				require.NoError(t, err)
				synctest.Wait()
				// When cancelling, the siblingsGroup context will be cancelled,
				// but as this routine is still running, the client connection
				// should not be closed yet.
				cancel()

				dependencyRoutineIsWaiting := false
				dependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					dependencyRoutineIsWaiting = true
					<-ctx.Done()
					// Wait for the other close to be called after
					// context cancellation.
					synctest.Wait()
					require.True(t, closeHasBeenCalled)
					return nil
				})
				synctest.Wait()
				require.True(t, dependencyRoutineIsWaiting)

				clientConnCloser.EXPECT().Close().Do(func() {
					require.False(t, closeHasBeenCalled)
					closeHasBeenCalled = true
				})
				return nil
			})
			require.NoError(t, err)
		})
	})
}
