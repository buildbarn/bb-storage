package capabilities_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestActionCacheUpdateEnabledClearingProvider(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseProvider := mock.NewMockCapabilitiesProvider(ctrl)
	authorizer := mock.NewMockAuthorizer(ctrl)
	provider := capabilities.NewActionCacheUpdateEnabledClearingProvider(baseProvider, authorizer)
	instanceName := util.Must(digest.NewInstanceName("hello"))

	t.Run("BackendFailure", func(t *testing.T) {
		baseProvider.EXPECT().GetCapabilities(ctx, instanceName).
			Return(nil, status.Error(codes.Unavailable, "Server not reachable"))

		_, err := provider.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server not reachable"), err)
	})

	t.Run("NoCacheCapabilities", func(t *testing.T) {
		// If the backend server provides no cache capabilities,
		// simply leave the response alone.
		baseProvider.EXPECT().GetCapabilities(ctx, instanceName).
			Return(&remoteexecution.ServerCapabilities{}, nil)

		response, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{}, response)
	})

	t.Run("SuccessTrue", func(t *testing.T) {
		// If the backend server provides cache capabilities, we
		// set the ActionCacheUpdateCapabilities field with the
		// appropriate value of UpdateEnabled.
		baseProvider.EXPECT().GetCapabilities(ctx, instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: digest.SupportedDigestFunctions,
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
				},
			}, nil)
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{util.Must(digest.NewInstanceName("hello"))}).Return([]error{nil})

		response, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
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
		baseProvider.EXPECT().GetCapabilities(ctx, instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: digest.SupportedDigestFunctions,
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
				},
			}, nil)
		authorizer.EXPECT().Authorize(gomock.Any(), []digest.InstanceName{util.Must(digest.NewInstanceName("hello"))}).Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})

		response, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
				ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
					UpdateEnabled: false,
				},
			},
		}, response)
	})
}
