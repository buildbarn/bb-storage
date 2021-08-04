package builder_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/builder"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUpdateEnabledTogglingBuildQueueGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBuildQueue := mock.NewMockBuildQueue(ctrl)
	instanceNameMatcher := mock.NewMockInstanceNameMatcher(ctrl)
	buildQueue := builder.NewUpdateEnabledTogglingBuildQueue(baseBuildQueue, instanceNameMatcher.Call)

	t.Run("InvalidInstanceName", func(t *testing.T) {
		_, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello/blobs/world",
		})
		require.Equal(t, status.Error(codes.InvalidArgument, "Invalid instance name \"hello/blobs/world\": Instance name contains reserved keyword \"blobs\""), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		baseBuildQueue.EXPECT().GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		}).Return(nil, status.Error(codes.Unavailable, "Server not reachable"))

		_, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		})
		require.Equal(t, status.Error(codes.Unavailable, "Server not reachable"), err)
	})

	t.Run("NoCacheCapabilities", func(t *testing.T) {
		// If the backend server provides no cache capabilities,
		// simply leave the response alone.
		baseBuildQueue.EXPECT().GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		}).Return(&remoteexecution.ServerCapabilities{}, nil)

		response, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		})
		require.NoError(t, err)
		require.Equal(t, &remoteexecution.ServerCapabilities{}, response)
	})

	t.Run("SuccessTrue", func(t *testing.T) {
		// If the backend server provides cache capabilities, we
		// set the ActionCacheUpdateCapabilities field with the
		// appropriate value of UpdateEnabled.
		baseBuildQueue.EXPECT().GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		}).Return(&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
			},
		}, nil)
		instanceNameMatcher.EXPECT().Call(digest.MustNewInstanceName("hello")).Return(true)

		response, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		})
		require.NoError(t, err)
		require.Equal(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
				ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
					UpdateEnabled: true,
				},
			},
		}, response)
	})

	t.Run("SuccessFalse", func(t *testing.T) {
		// Same as the test above, except to check that the
		// value 'false' is filled in.
		baseBuildQueue.EXPECT().GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		}).Return(&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
			},
		}, nil)
		instanceNameMatcher.EXPECT().Call(digest.MustNewInstanceName("hello")).Return(false)

		response, err := buildQueue.GetCapabilities(ctx, &remoteexecution.GetCapabilitiesRequest{
			InstanceName: "hello",
		})
		require.NoError(t, err)
		require.Equal(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
				ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
					UpdateEnabled: false,
				},
			},
		}, response)
	})
}
