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

func TestMergingProviderAPIVersionIntersection(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	t.Run("NoProvidersWithAPIVersions", func(t *testing.T) {
		// When no providers declare API versions, the result should not have API versions
		// (server.go will set defaults)
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should merge capabilities but not set API versions
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("SingleProviderWithAPIVersions", func(t *testing.T) {
		// When only one provider declares API versions, use those versions
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
			HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("MultipleProvidersWithOverlappingVersions", func(t *testing.T) {
		// Test normal intersection: provider1 supports 2.0-2.2, provider2 supports 2.1-2.3
		// Result should be intersection: 2.1-2.2
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 1},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 3},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			LowApiVersion:  &semver.SemVer{Major: 2, Minor: 1}, // MAX of (2.0, 2.1)
			HighApiVersion: &semver.SemVer{Major: 2, Minor: 2}, // MIN of (2.2, 2.3)
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("API20OnlyProvider", func(t *testing.T) {
		// Test case for API 2.0 only provider (like the real-world scenario)
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider that only supports API 2.0
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 0},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider that supports broader range
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 3},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Result should be intersection: 2.0-2.0 (only API 2.0 supported)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
			HighApiVersion: &semver.SemVer{Major: 2, Minor: 0},
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("NoOverlappingVersions", func(t *testing.T) {
		// When providers have no overlapping version ranges, fall back to no API versions
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider supports 2.0-2.1
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 1},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider supports 2.2-2.3 (no overlap)
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 2},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 3},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should merge capabilities but not set API versions due to no overlap
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("MixedScenario", func(t *testing.T) {
		// Mixed scenario: some providers declare versions, others don't
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)
		provider3 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2, provider3})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider with API versions
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider without API versions
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		// Another provider with API versions
		provider3.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:  &semver.SemVer{Major: 2, Minor: 1},
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 3},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					MaxBatchTotalSizeBytes: 1024,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should intersect only providers with versions (2.0-2.2 ∩ 2.1-2.3 = 2.1-2.2)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			LowApiVersion:  &semver.SemVer{Major: 2, Minor: 1},
			HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
				MaxBatchTotalSizeBytes: 1024,
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("PartialAPIVersions", func(t *testing.T) {
		// Test providers with only LowApiVersion or only HighApiVersion
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider with only LowApiVersion
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion: &semver.SemVer{Major: 2, Minor: 1},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider with only HighApiVersion
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should handle partial API versions correctly
		// Provider 1: only low=2.1 -> effective range 2.1-Inf (at least 2.1)
		// Provider 2: only high=2.2 -> effective range -Inf-2.2 (up to 2.2)
		// Intersection: 2.1-2.2
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			LowApiVersion:  &semver.SemVer{Major: 2, Minor: 1},
			HighApiVersion: &semver.SemVer{Major: 2, Minor: 2},
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("DeprecatedApiVersionHandling", func(t *testing.T) {
		// Test DeprecatedApiVersion handling - should take the maximum value
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)
		provider3 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2, provider3})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider with DeprecatedApiVersion 2.0
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 0},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider without DeprecatedApiVersion
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		// Provider with higher DeprecatedApiVersion 2.2 (should be the max)
		provider3.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 2},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					MaxBatchTotalSizeBytes: 1024,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should use the maximum DeprecatedApiVersion (2.2)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			DeprecatedApiVersion: &semver.SemVer{Major: 2, Minor: 2}, // MAX of (2.0, nil, 2.2)
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
				MaxBatchTotalSizeBytes: 1024,
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("DeprecatedApiVersionWithAPIVersions", func(t *testing.T) {
		// Test DeprecatedApiVersion handling combined with API version intersection
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider with both API versions and DeprecatedApiVersion
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:        &semver.SemVer{Major: 2, Minor: 0},
				HighApiVersion:       &semver.SemVer{Major: 2, Minor: 2},
				DeprecatedApiVersion: &semver.SemVer{Major: 1, Minor: 5},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider with different API versions and higher DeprecatedApiVersion
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				LowApiVersion:        &semver.SemVer{Major: 2, Minor: 1},
				HighApiVersion:       &semver.SemVer{Major: 2, Minor: 3},
				DeprecatedApiVersion: &semver.SemVer{Major: 1, Minor: 8},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should intersect API versions (2.1-2.2) and use max DeprecatedApiVersion (1.8)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			LowApiVersion:        &semver.SemVer{Major: 2, Minor: 1}, // MAX of (2.0, 2.1)
			HighApiVersion:       &semver.SemVer{Major: 2, Minor: 2}, // MIN of (2.2, 2.3)
			DeprecatedApiVersion: &semver.SemVer{Major: 1, Minor: 8}, // MAX of (1.5, 1.8)
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})

	t.Run("OnlyDeprecatedApiVersion", func(t *testing.T) {
		// Test when only one provider has DeprecatedApiVersion
		provider1 := mock.NewMockCapabilitiesProvider(ctrl)
		provider2 := mock.NewMockCapabilitiesProvider(ctrl)

		provider := capabilities.NewMergingProvider([]capabilities.Provider{provider1, provider2})
		instanceName := util.Must(digest.NewInstanceName("example"))

		// Provider with only DeprecatedApiVersion
		provider1.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				DeprecatedApiVersion: &semver.SemVer{Major: 1, Minor: 9},
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
					},
				},
			}, nil)

		// Provider without any API version info
		provider2.EXPECT().GetCapabilities(gomock.Any(), instanceName).
			Return(&remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					ExecEnabled: true,
				},
			}, nil)

		serverCapabilities, err := provider.GetCapabilities(ctx, instanceName)
		require.NoError(t, err)

		// Should preserve the single DeprecatedApiVersion value
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			DeprecatedApiVersion: &semver.SemVer{Major: 1, Minor: 9},
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
			},
			ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
				ExecEnabled: true,
			},
		}, serverCapabilities)
	})
}
