package grpcclients_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	bb_zstd "github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

func newTestZstdPool(maxEncoders, maxDecoders int64) bb_zstd.Pool {
	return bb_zstd.NewBoundedPool(
		maxEncoders, maxDecoders,
		[]zstd.EOption{zstd.WithEncoderConcurrency(1)},
		[]zstd.DOption{zstd.WithDecoderConcurrency(1)},
	)
}

func expectGetCapabilitiesWithZSTD(client *mock.MockClientConnInterface) {
	client.EXPECT().Invoke(
		gomock.Any(),
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
		proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
				SupportedCompressors: []remoteexecution.Compressor_Value{
					remoteexecution.Compressor_ZSTD,
				},
				SplitBlobSupport:  true,
				SpliceBlobSupport: true,
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 256 << 10,
					HorizonSizeBytes:  8 * 256 << 10,
				},
			},
		})
		return nil
	}).AnyTimes()
}

func TestCSBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	pool := newTestZstdPool(1, 1)
	blobAccess := grpcclients.NewCSBlobAccess(client, pool, false)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("InitialFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(status.Error(codes.Internal, "Failed to create outgoing connection"))

		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to create outgoing connection"),
			blobAccess.Put(ctx, blobDigest, chunk.NewChunk(pool, []byte("Hello"))))
	})

	t.Run("ServerFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchUpdateBlobsResponse)
			resp.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.Unavailable, "Disk on fire").Proto(),
				},
			}
			return nil
		})

		testutil.RequireEqualStatus(t,
			status.Error(codes.Unavailable, "Disk on fire"),
			blobAccess.Put(ctx, blobDigest, chunk.NewChunk(pool, []byte("Hello"))))
	})

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			testutil.EqProto(t, &remoteexecution.BatchUpdateBlobsRequest{
				InstanceName:   "hello",
				DigestFunction: remoteexecution.DigestFunction_MD5,
				Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
					{
						Digest:     blobDigest.GetProto(),
						Data:       []byte("Hello"),
						Compressor: remoteexecution.Compressor_IDENTITY,
					},
				},
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchUpdateBlobsResponse)
			resp.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.OK, "").Proto(),
				},
			}
			return nil
		})

		err := blobAccess.Put(ctx, blobDigest, chunk.NewChunk(pool, []byte("Hello")))
		require.NoError(t, err)
	})
}

func TestCSBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	pool := newTestZstdPool(1, 1)
	blobAccess := grpcclients.NewCSBlobAccess(client, pool, false)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			testutil.EqProto(t, &remoteexecution.BatchReadBlobsRequest{
				InstanceName:          "hello",
				Digests:               []*remoteexecution.Digest{blobDigest.GetProto()},
				AcceptableCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_IDENTITY},
				DigestFunction:        remoteexecution.DigestFunction_MD5,
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchReadBlobsResponse)
			resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
				{
					Digest:     blobDigest.GetProto(),
					Data:       []byte("Hello"),
					Status:     status.New(codes.OK, "").Proto(),
					Compressor: remoteexecution.Compressor_IDENTITY,
				},
			}
			return nil
		})

		chunk, err := blobAccess.Get(ctx, blobDigest)
		require.NoError(t, err)
		data, err := chunk.GetBytes(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("InitialFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(status.Error(codes.Internal, "Failed to create outgoing connection"))

		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to create outgoing connection"),
			err)
	})

	t.Run("ServerFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchReadBlobsResponse)
			resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.Internal, "Lost connection to server").Proto(),
				},
			}
			return nil
		})

		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Lost connection to server"),
			err)
	})
}

func TestCSBlobAccessGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	pool := newTestZstdPool(1, 1)
	blobAccess := grpcclients.NewCSBlobAccess(client, pool, false)

	t.Run("BackendFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
			testutil.EqProto(t, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "hello/world",
			}),
			gomock.Any(),
			gomock.Any(),
		).Return(status.Error(codes.Unavailable, "Server offline"))

		_, err := blobAccess.GetCapabilities(ctx, util.Must(digest.NewInstanceName("hello/world")))
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), err)
	})

	t.Run("OnlyExecution", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
			testutil.EqProto(t, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "hello/world",
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
				DeprecatedApiVersion: &semver.SemVer{Major: 2},
				LowApiVersion:        &semver.SemVer{Major: 2},
				HighApiVersion:       &semver.SemVer{Major: 2},
			})
			return nil
		})

		_, err := blobAccess.GetCapabilities(ctx, util.Must(digest.NewInstanceName("hello/world")))
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name \"hello/world\" does not support remote caching"), err)
	})

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
			testutil.EqProto(t, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "hello/world",
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
						remoteexecution.DigestFunction_VSO,
					},
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
					MaxBatchTotalSizeBytes:      1 << 20,
					SymlinkAbsolutePathStrategy: remoteexecution.SymlinkAbsolutePathStrategy_ALLOWED,
					SupportedCompressors: []remoteexecution.Compressor_Value{
						remoteexecution.Compressor_ZSTD,
					},
					SplitBlobSupport:  true,
					SpliceBlobSupport: true,
					RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
						MinChunkSizeBytes: 256 << 10,
						HorizonSizeBytes:  8 * 256 << 10,
					},
				},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
				DeprecatedApiVersion: &semver.SemVer{Major: 2},
				LowApiVersion:        &semver.SemVer{Major: 2},
				HighApiVersion:       &semver.SemVer{Major: 2},
			})
			return nil
		})

		serverCapabilities, err := blobAccess.GetCapabilities(ctx, util.Must(digest.NewInstanceName("hello/world")))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 256 << 10,
					HorizonSizeBytes:  8 * 256 << 10,
				},
			},
			DeprecatedApiVersion: &semver.SemVer{Major: 2},
			LowApiVersion:        &semver.SemVer{Major: 2},
			HighApiVersion:       &semver.SemVer{Major: 2},
		}, serverCapabilities)
	})
}

func TestCSBlobAccessPutWithCompression(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	pool := newTestZstdPool(1, 1)
	blobAccess := grpcclients.NewCSBlobAccess(client, pool, true)

	expectGetCapabilitiesWithZSTD(client)
	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

	t.Run("SuccessWithCompression", func(t *testing.T) {
		largeData := make([]byte, 1000)
		for i := range largeData {
			largeData[i] = byte('A' + (i % 26))
		}

		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			r := req.(*remoteexecution.BatchUpdateBlobsRequest)
			require.Len(t, r.Requests, 1)
			require.Equal(t, remoteexecution.Compressor_ZSTD, r.Requests[0].Compressor)
			require.Less(t, len(r.Requests[0].Data), 1000, "Compressed data should be smaller than original")

			resp := reply.(*remoteexecution.BatchUpdateBlobsResponse)
			resp.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.OK, "").Proto(),
				},
			}
			return nil
		})

		err := blobAccess.Put(ctx, blobDigest, chunk.NewChunk(pool, largeData))
		require.NoError(t, err)
	})
}

func TestCSBlobAccessGetWithCompression(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	pool := newTestZstdPool(1, 1)
	blobAccess := grpcclients.NewCSBlobAccess(client, pool, true)

	expectGetCapabilitiesWithZSTD(client)

	t.Run("SuccessWithCompression", func(t *testing.T) {
		expectedData := make([]byte, 1000)
		for i := range expectedData {
			expectedData[i] = byte('A' + (i % 26))
		}
		largeDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(expectedData, nil)

		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			testutil.EqProto(t, &remoteexecution.BatchReadBlobsRequest{
				InstanceName:          "hello",
				Digests:               []*remoteexecution.Digest{largeDigest.GetProto()},
				AcceptableCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
				DigestFunction:        remoteexecution.DigestFunction_MD5,
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchReadBlobsResponse)
			resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
				{
					Digest:     largeDigest.GetProto(),
					Data:       compressedData,
					Status:     status.New(codes.OK, "").Proto(),
					Compressor: remoteexecution.Compressor_ZSTD,
				},
			}
			return nil
		})

		chunk, err := blobAccess.Get(ctx, largeDigest)
		require.NoError(t, err)
		data, err := chunk.GetBytes(ctx)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)
	})
}

func TestCSBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	pool := newTestZstdPool(1, 1)
	blobAccess := grpcclients.NewCSBlobAccess(client, pool, false)

	digest1 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digest2 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			r := req.(*remoteexecution.FindMissingBlobsRequest)
			require.Equal(t, "hello", r.InstanceName)
			require.Len(t, r.BlobDigests, 2)
			resp := reply.(*remoteexecution.FindMissingBlobsResponse)
			resp.MissingBlobDigests = []*remoteexecution.Digest{digest2.GetProto()}
			return nil
		})

		digestsToSearch := digest.NewSetBuilder(0).Add(digest1).Add(digest2).Build()
		missing, err := blobAccess.FindMissing(ctx, digestsToSearch)
		require.NoError(t, err)
		require.Equal(t, digest.NewSetBuilder(0).Add(digest2).Build(), missing)
	})
}
