package capabilities_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	provider := mock.NewMockCapabilitiesProvider(ctrl)
	server := capabilities.NewServer(provider)

	t.Run("InvalidInstanceName", func(t *testing.T) {
		_, err := server.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello/blobs/world",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid instance name \"hello/blobs/world\": Instance name contains reserved keyword \"blobs\""), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		provider.EXPECT().GetCapabilities(ctx, digest.MustNewInstanceName("hello/world")).
			Return(nil, status.Error(codes.Internal, "Server offline"))

		_, err := server.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello/world",
		})
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server offline"), err)
	})

	t.Run("Success", func(t *testing.T) {
		provider.EXPECT().GetCapabilities(ctx, digest.MustNewInstanceName("hello/world")).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction: remoteexecution.DigestFunction_SHA256,
					ExecEnabled:    true,
				},
			}, nil)

		serverCapabilities, err := server.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello/world",
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				DigestFunction: remoteexecution.DigestFunction_SHA256,
				ExecEnabled:    true,
			},
			LowApiVersion:  &semver.SemVer{Major: 2},
			HighApiVersion: &semver.SemVer{Major: 2},
		}, serverCapabilities)
	})
}
