package grpc_test

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestDeduplicatingClientFactory(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseClientFactory := mock.NewMockClientFactory(ctrl)
	clientFactory := bb_grpc.NewDeduplicatingClientFactory(baseClientFactory)

	t.Run("Nil", func(t *testing.T) {
		// Even though BaseClientFactory doesn't create gRPC
		// clients when the provided configuration is nil, it
		// shouldn't cause DeduplicatingClientFactory to
		// misbehave.
		synctest.Test(t, func(t *testing.T) {
			baseClientFactory.EXPECT().NewClientFromConfiguration(nil, gomock.Any()).
				Return(nil, status.Error(codes.InvalidArgument, "No gRPC client configuration provided"))

			_, err := clientFactory.NewClientFromConfiguration(nil, nil)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "No gRPC client configuration provided"), err)
		})
	})

	t.Run("CreationError", func(t *testing.T) {
		// gRPC client creation failures should not be cached.
		synctest.Test(t, func(t *testing.T) {
			baseClientFactory.EXPECT().NewClientFromConfiguration(
				testutil.EqProto(
					t,
					&configuration.ClientConfiguration{
						Address: "example.com:123456",
					},
				),
				gomock.Any(),
			).Return(nil, status.Error(codes.InvalidArgument, "Invalid port number: 123456")).Times(2)

			_, err := clientFactory.NewClientFromConfiguration(
				&configuration.ClientConfiguration{
					Address: "example.com:123456",
				},
				nil,
			)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid port number: 123456"), err)
			_, err = clientFactory.NewClientFromConfiguration(
				&configuration.ClientConfiguration{
					Address: "example.com:123456",
				},
				nil,
			)
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid port number: 123456"), err)
		})
	})

	t.Run("Success", func(t *testing.T) {
		// Upon success, gRPC clients should be cached, so that
		// successive calls return the same client connection.
		synctest.Test(t, func(t *testing.T) {
			testContext, cancelTestContext := context.WithCancel(ctx)
			err := program.RunLocal(testContext, func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
				eqTarget1 := testutil.EqProto(t, &configuration.ClientConfiguration{
					Address: "example.com:1",
				})
				eqTarget2 := testutil.EqProto(t, &configuration.ClientConfiguration{
					Address: "example.com:2",
				})

				client1 := mock.NewMockClientConnInterface(ctrl)
				client2 := mock.NewMockClientConnInterface(ctrl)

				var ctx1 context.Context
				var ctx2 context.Context

				baseClientFactory.EXPECT().NewClientFromConfiguration(eqTarget1, gomock.Any()).DoAndReturn(
					func(_ *configuration.ClientConfiguration, group program.Group) (grpc.ClientConnInterface, error) {
						group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
							ctx1 = ctx
							return nil
						})
						return client1, nil
					},
				)
				baseClientFactory.EXPECT().NewClientFromConfiguration(eqTarget2, gomock.Any()).DoAndReturn(
					func(_ *configuration.ClientConfiguration, group program.Group) (grpc.ClientConnInterface, error) {
						group.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
							ctx2 = ctx
							return nil
						})
						return client2, nil
					},
				)

				for i := 0; i < 1; i++ {
					client, err := clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
						Address: "example.com:1",
					}, siblingsGroup)
					require.NoError(t, err)
					require.Equal(t, client1, client)

					// Client2 is created in both the siblings and dependencies groups.
					client, err = clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
						Address: "example.com:2",
					}, siblingsGroup)
					require.NoError(t, err)
					require.Equal(t, client2, client)

					client, err = clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
						Address: "example.com:2",
					}, dependenciesGroup)
					require.NoError(t, err)
					require.Equal(t, client2, client)
				}

				synctest.Wait()
				require.NoError(t, ctx1.Err())
				require.NoError(t, ctx2.Err())
				// Cancel all client1 groups, but keep the
				// dependenciesGroup context for client2 alive
				// by not returning from this function yet.
				cancelTestContext()
				synctest.Wait()
				<-ctx1.Done()
				require.NoError(t, ctx2.Err())
				// Observe the state change for ctx2.
				ctx2ObserverStarted := make(chan struct{})
				dependenciesGroup.Go(func(ctx context.Context, siblingsGroup, dependenciesGroup program.Group) error {
					synctest.Wait()
					require.NoError(t, ctx2.Err())
					close(ctx2ObserverStarted)
					// When the siblings group is done, ctx will be cancelled and then ctx2 as well.
					<-ctx.Done()
					<-ctx2.Done()
					return nil
				})
				// Wait for the observer to start.
				<-ctx2ObserverStarted
				return nil
			})
			require.NoError(t, err)
		})
	})
}
