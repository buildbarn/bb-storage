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

func TestMergingProviderZero(t *testing.T) {
	provider := capabilities.NewMergingProvider(nil)
	instanceName := util.Must(digest.NewInstanceName("example"))

	t.Run("Failure", func(t *testing.T) {
		_, err := provider.GetCapabilities(context.Background(), instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "No capabilities providers registered"), err)
	})
}

func TestMergingProviderOne(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseProvider := mock.NewMockCapabilitiesProvider(ctrl)
	provider := capabilities.NewMergingProvider([]capabilities.Provider{baseProvider})
	instanceName := util.Must(digest.NewInstanceName("example"))

	t.Run("Success", func(t *testing.T) {
		baseProvider.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				DigestFunction:  remoteexecution.DigestFunction_SHA256,
				DigestFunctions: digest.SupportedDigestFunctions,
				ExecEnabled:     true,
			},
		}, serverCapabilities)
	})
}

func TestMergingProviderMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionCache := mock.NewMockCapabilitiesProvider(ctrl)
	contentAddressableStorage := mock.NewMockCapabilitiesProvider(ctrl)
	scheduler := mock.NewMockCapabilitiesProvider(ctrl)
	provider := capabilities.NewMergingProvider([]capabilities.Provider{
		actionCache,
		contentAddressableStorage,
		scheduler,
	})
	instanceName := util.Must(digest.NewInstanceName("example"))

	t.Run("AllNotFound", func(t *testing.T) {
		actionCache.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(nil, status.Error(codes.NotFound, "Unknown instance name: \"example\""))
		contentAddressableStorage.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(nil, status.Error(codes.NotFound, "Unknown instance name: \"example\""))
		scheduler.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(nil, status.Error(codes.NotFound, "Unknown instance name: \"example\""))

		_, err := provider.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Unknown instance name: \"example\""), err)
	})

	t.Run("BackendFailure", func(t *testing.T) {
		actionCache.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
				},
			}, nil)
		contentAddressableStorage.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(nil, status.Error(codes.NotFound, "Unknown instance name: \"example\""))
		scheduler.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(nil, status.Error(codes.Internal, "Server offline"))

		_, err := provider.GetCapabilities(ctx, instanceName)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server offline"), err)
	})

	t.Run("CacheOnly", func(t *testing.T) {
		actionCache.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
				},
			}, nil)
		contentAddressableStorage.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)
		scheduler.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(nil, status.Error(codes.NotFound, "Unknown instance name: \"example\""))

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
					UpdateEnabled: true,
				},
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
		}, serverCapabilities)
	})

	t.Run("CacheAndExecution", func(t *testing.T) {
		actionCache.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
				},
			}, nil)
		contentAddressableStorage.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)
		scheduler.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
					UpdateEnabled: true,
				},
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				DigestFunction:  remoteexecution.DigestFunction_SHA256,
				DigestFunctions: digest.SupportedDigestFunctions,
				ExecEnabled:     true,
			},
		}, serverCapabilities)
	})
}

func TestMergingProviderWithCompression(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobAccessProvider := mock.NewMockCapabilitiesProvider(ctrl)

	compressionProvider := capabilities.NewStaticProvider(&remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			SupportedCompressors: []remoteexecution.Compressor_Value{
				remoteexecution.Compressor_IDENTITY,
				remoteexecution.Compressor_ZSTD,
			},
		},
	})

	provider := capabilities.NewMergingProvider([]capabilities.Provider{
		blobAccessProvider,
		compressionProvider,
	})
	instanceName := util.Must(digest.NewInstanceName("example"))

	blobAccessProvider.EXPECT().GetCapabilities(gomock.Any(), instanceName).
		Return(&remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
					remoteexecution.DigestFunction_SHA1,
				},
			},
		}, nil)

	serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: []remoteexecution.DigestFunction_Value{
				remoteexecution.DigestFunction_SHA256,
				remoteexecution.DigestFunction_SHA1,
			},
			SupportedCompressors: []remoteexecution.Compressor_Value{
				remoteexecution.Compressor_IDENTITY,
				remoteexecution.Compressor_ZSTD,
			},
		},
	}, serverCapabilities)
}
