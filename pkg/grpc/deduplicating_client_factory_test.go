package grpc_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	bb_grpc "github.com/buildbarn/bb-storage/pkg/grpc"
	configuration "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeduplicatingClientFactory(t *testing.T) {
	ctrl := gomock.NewController(t)

	baseClientFactory := mock.NewMockClientFactory(ctrl)
	clientFactory := bb_grpc.NewDeduplicatingClientFactory(baseClientFactory)

	t.Run("Nil", func(t *testing.T) {
		// Even though BaseClientFactory doesn't create gRPC
		// clients when the provided configuration is nil, it
		// shouldn't cause DeduplicatingClientFactory to
		// misbehave.
		baseClientFactory.EXPECT().NewClientFromConfiguration(nil).
			Return(nil, status.Error(codes.InvalidArgument, "No gRPC client configuration provided"))

		_, err := clientFactory.NewClientFromConfiguration(nil)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "No gRPC client configuration provided"), err)
	})

	t.Run("CreationError", func(t *testing.T) {
		// gRPC client creation failures should not be cached.
		baseClientFactory.EXPECT().NewClientFromConfiguration(
			testutil.EqProto(
				t,
				&configuration.ClientConfiguration{
					Address: "example.com:123456",
				})).Return(nil, status.Error(codes.InvalidArgument, "Invalid port number: 123456")).Times(2)

		_, err := clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
			Address: "example.com:123456",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid port number: 123456"), err)
		_, err = clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
			Address: "example.com:123456",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid port number: 123456"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Upon success, gRPC clients should be cached, so that
		// successive calls return the same client connection.
		client1 := mock.NewMockClientConnInterface(ctrl)
		baseClientFactory.EXPECT().NewClientFromConfiguration(
			testutil.EqProto(
				t,
				&configuration.ClientConfiguration{
					Address: "example.com:1",
				})).Return(client1, nil)

		client2 := mock.NewMockClientConnInterface(ctrl)
		baseClientFactory.EXPECT().NewClientFromConfiguration(
			testutil.EqProto(
				t,
				&configuration.ClientConfiguration{
					Address: "example.com:2",
				})).Return(client2, nil)

		for i := 0; i < 10; i++ {
			client, err := clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
				Address: "example.com:1",
			})
			require.NoError(t, err)
			require.Equal(t, client1, client)

			client, err = clientFactory.NewClientFromConfiguration(&configuration.ClientConfiguration{
				Address: "example.com:2",
			})
			require.NoError(t, err)
			require.Equal(t, client2, client)
		}
	})
}
