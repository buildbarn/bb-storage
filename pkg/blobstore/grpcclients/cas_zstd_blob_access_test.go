package grpcclients_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

func TestCASWithZstdBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)

	// Set threshold to 1 byte so our 5-byte test blob uses compression.
	blobAccess := grpcclients.NewCASWithZstdBlobAccess(client, uuidGenerator.Call, 10, 1)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	testUUID := uuid.Must(uuid.Parse("7d659e5f-0e4b-48f0-ad9f-3489db6e103b"))

	t.Run("InitialFailure", func(t *testing.T) {
		uuidGenerator.EXPECT().Call().Return(testUUID, nil)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(nil, status.Error(codes.Internal, "Failed to create outgoing connection"))
		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().Close().AnyTimes()

		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to create outgoing connection"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromReaderAt(r, 5)))
	})

	t.Run("ReadFailure", func(t *testing.T) {
		clientStream := mock.NewMockClientStream(ctrl)
		uuidGenerator.EXPECT().Call().Return(testUUID, nil)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(clientStream, nil)
		r := mock.NewMockFileReader(ctrl)

		r.EXPECT().ReadAt(gomock.Len(5), int64(0)).Return(0, status.Error(codes.Internal, "Disk on fire"))
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.True(t, req.FinishWrite, "Should be a finish message during cleanup")
			return nil
		}).AnyTimes()
		clientStream.EXPECT().CloseSend().Return(status.Error(codes.Canceled, "Request canceled by client")).AnyTimes()
		r.EXPECT().Close()

		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Disk on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromReaderAt(r, 5)))
	})

	t.Run("SendFailureInitial", func(t *testing.T) {
		clientStream := mock.NewMockClientStream(ctrl)
		uuidGenerator.EXPECT().Call().Return(testUUID, nil)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(clientStream, nil)
		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Len(5), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, nil
		})
		r.EXPECT().Close()
		// For ZSTD, the data will be compressed, so we can't predict the exact bytes.
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			if !req.FinishWrite {
				require.Equal(t, "hello/uploads/7d659e5f-0e4b-48f0-ad9f-3489db6e103b/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/5", req.ResourceName)
				require.Equal(t, int64(0), req.WriteOffset)
				require.NotEmpty(t, req.Data)
				return io.EOF
			} else {
				return nil
			}
		}).AnyTimes()
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(status.Error(codes.Unavailable, "Lost connection to server")).AnyTimes()
		clientStream.EXPECT().CloseSend().Return(nil).AnyTimes()

		testutil.RequireEqualStatus(t,
			status.Error(codes.Unknown, "EOF"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromReaderAt(r, 5)))
	})

	t.Run("SendFailureFinal", func(t *testing.T) {
		clientStream := mock.NewMockClientStream(ctrl)
		uuidGenerator.EXPECT().Call().Return(testUUID, nil)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(clientStream, nil)
		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Len(5), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, nil
		})
		r.EXPECT().Close()
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.Equal(t, "hello/uploads/7d659e5f-0e4b-48f0-ad9f-3489db6e103b/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/5", req.ResourceName)
			require.Equal(t, int64(0), req.WriteOffset)
			require.NotEmpty(t, req.Data)
			return nil
		})
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.True(t, req.FinishWrite)
			require.Greater(t, req.WriteOffset, int64(0))
			return io.EOF
		})
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(status.Error(codes.Unavailable, "Lost connection to server")).AnyTimes()
		clientStream.EXPECT().CloseSend().Return(nil).AnyTimes()

		testutil.RequireEqualStatus(t,
			status.Error(codes.Unknown, "EOF"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromReaderAt(r, 5)))
	})

	t.Run("CloseAndRecvFailure", func(t *testing.T) {
		clientStream := mock.NewMockClientStream(ctrl)
		uuidGenerator.EXPECT().Call().Return(testUUID, nil)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(clientStream, nil)
		r := mock.NewMockFileReader(ctrl)
		r.EXPECT().ReadAt(gomock.Len(5), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, nil
		})
		r.EXPECT().Close()

		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.Equal(t, "hello/uploads/7d659e5f-0e4b-48f0-ad9f-3489db6e103b/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/5", req.ResourceName)
			require.Equal(t, int64(0), req.WriteOffset)
			require.NotEmpty(t, req.Data)
			return nil
		})
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.True(t, req.FinishWrite)
			require.Greater(t, req.WriteOffset, int64(0))
			return nil
		})
		clientStream.EXPECT().CloseSend().Return(status.Error(codes.Unavailable, "Lost connection to server"))

		testutil.RequireEqualStatus(t,
			status.Error(codes.Unavailable, "Lost connection to server"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromReaderAt(r, 5)))
	})

	t.Run("SuccessSmallBlob", func(t *testing.T) {
		ctrl2, ctx2 := gomock.WithContext(context.Background(), t)
		client2 := mock.NewMockClientConnInterface(ctrl2)
		uuidGenerator2 := mock.NewMockUUIDGenerator(ctrl2)
		blobAccess2 := grpcclients.NewCASWithZstdBlobAccess(client2, uuidGenerator2.Call, 10, 50)

		smallDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
		testUUID2 := uuid.Must(uuid.Parse("7d659e5f-0e4b-48f0-ad9f-3489db6e103b"))

		clientStream := mock.NewMockClientStream(ctrl2)
		uuidGenerator2.EXPECT().Call().Return(testUUID2, nil)
		client2.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(clientStream, nil)
		r := mock.NewMockFileReader(ctrl2)
		r.EXPECT().ReadAt(gomock.Len(5), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, nil
		})
		r.EXPECT().Close()
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &bytestream.WriteRequest{
			ResourceName: "hello/uploads/7d659e5f-0e4b-48f0-ad9f-3489db6e103b/blobs/8b1a9953c4611296a827abf8c47804d7/5",
			WriteOffset:  0,
			Data:         []byte("Hello"),
		}))
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &bytestream.WriteRequest{
			WriteOffset: 5,
			FinishWrite: true,
		}))
		clientStream.EXPECT().CloseSend().Return(nil)
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			resp := m.(*bytestream.WriteResponse)
			resp.CommittedSize = 5
			return nil
		})

		err := blobAccess2.Put(ctx2, smallDigest, buffer.NewValidatedBufferFromReaderAt(r, 5))
		require.NoError(t, err)
	})

	t.Run("SuccessLargeBlob", func(t *testing.T) {
		ctrl3, ctx3 := gomock.WithContext(context.Background(), t)
		client3 := mock.NewMockClientConnInterface(ctrl3)
		uuidGenerator3 := mock.NewMockUUIDGenerator(ctrl3)
		blobAccess3 := grpcclients.NewCASWithZstdBlobAccess(client3, uuidGenerator3.Call, 100, 50)

		largeData := make([]byte, 1000)
		for i := range largeData {
			largeData[i] = byte('A' + (i % 26))
		}
		largeDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)
		testUUID3 := uuid.Must(uuid.Parse("7d659e5f-0e4b-48f0-ad9f-3489db6e103b"))

		clientStream := mock.NewMockClientStream(ctrl3)
		uuidGenerator3.EXPECT().Call().Return(testUUID3, nil)
		client3.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Write").
			Return(clientStream, nil)
		r := mock.NewMockFileReader(ctrl3)
		r.EXPECT().ReadAt(gomock.Len(1000), int64(0)).DoAndReturn(func(p []byte, off int64) (int, error) {
			copy(p, largeData)
			return 1000, nil
		})
		r.EXPECT().Close()

		var compressedSize int64
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.Equal(t, "hello/uploads/7d659e5f-0e4b-48f0-ad9f-3489db6e103b/compressed-blobs/zstd/1411ffd5854fa029dc4d231aa89311eb/1000", req.ResourceName)
			require.Equal(t, int64(0), req.WriteOffset)
			require.NotEmpty(t, req.Data)
			require.Less(t, len(req.Data), 1000, "Compressed data should be smaller than original")
			compressedSize = int64(len(req.Data))
			return nil
		})
		clientStream.EXPECT().SendMsg(gomock.Any()).DoAndReturn(func(msg interface{}) error {
			req := msg.(*bytestream.WriteRequest)
			require.True(t, req.FinishWrite)
			require.Equal(t, compressedSize, req.WriteOffset)
			return nil
		})
		clientStream.EXPECT().CloseSend().Return(nil)
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			resp := m.(*bytestream.WriteResponse)
			resp.CommittedSize = compressedSize
			return nil
		})

		err := blobAccess3.Put(ctx3, largeDigest, buffer.NewValidatedBufferFromReaderAt(r, 1000))
		require.NoError(t, err)
	})
}

func TestCASWithZstdBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)

	t.Run("SuccessSmallBlob", func(t *testing.T) {
		blobAccess := grpcclients.NewCASWithZstdBlobAccess(client, uuidGenerator.Call, 10, 50)

		smallDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Read").
			Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &bytestream.ReadRequest{
			ResourceName: "hello/blobs/8b1a9953c4611296a827abf8c47804d7/5",
			ReadOffset:   0,
			ReadLimit:    0,
		})).Return(nil)
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			resp := m.(*bytestream.ReadResponse)
			resp.Data = []byte("Hello")
			return nil
		})
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(io.EOF).AnyTimes()
		clientStream.EXPECT().CloseSend().Return(nil)

		buffer := blobAccess.Get(ctx, smallDigest)
		data, err := buffer.ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SuccessLargeBlob", func(t *testing.T) {
		blobAccess := grpcclients.NewCASWithZstdBlobAccess(client, uuidGenerator.Call, 100, 50)

		expectedData := make([]byte, 1000)
		for i := range expectedData {
			expectedData[i] = byte('A' + (i % 26))
		}
		largeDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Read").
			Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &bytestream.ReadRequest{
			ResourceName: "hello/compressed-blobs/zstd/1411ffd5854fa029dc4d231aa89311eb/1000",
			ReadOffset:   0,
			ReadLimit:    0,
		})).Return(nil)

		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(expectedData, nil)
		require.Less(t, len(compressedData), len(expectedData), "Compressed data should be smaller than original")

		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			resp := m.(*bytestream.ReadResponse)
			resp.Data = compressedData
			return nil
		})
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(io.EOF).AnyTimes()
		clientStream.EXPECT().CloseSend().Return(nil)

		buffer := blobAccess.Get(ctx, largeDigest)
		data, err := buffer.ToByteSlice(1500)
		require.NoError(t, err)
		require.Equal(t, expectedData, data)
	})

	t.Run("InitialFailure", func(t *testing.T) {
		blobAccess := grpcclients.NewCASWithZstdBlobAccess(client, uuidGenerator.Call, 10, 1)
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Read").
			Return(nil, status.Error(codes.Internal, "Failed to create outgoing connection"))

		buffer := blobAccess.Get(ctx, blobDigest)
		_, err := buffer.ToByteSlice(1000)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to create outgoing connection"),
			err)
	})

	t.Run("ReceiveFailure", func(t *testing.T) {
		blobAccess := grpcclients.NewCASWithZstdBlobAccess(client, uuidGenerator.Call, 10, 1)
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(gomock.Any(), gomock.Any(), "/google.bytestream.ByteStream/Read").
			Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &bytestream.ReadRequest{
			ResourceName: "hello/compressed-blobs/zstd/8b1a9953c4611296a827abf8c47804d7/5",
			ReadOffset:   0,
			ReadLimit:    0,
		})).Return(nil)
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(status.Error(codes.Internal, "Lost connection to server")).AnyTimes()
		clientStream.EXPECT().CloseSend().Return(nil)

		buffer := blobAccess.Get(ctx, blobDigest)
		_, err := buffer.ToByteSlice(1000)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Lost connection to server"),
			err)
	})
}

func TestCASWithZstdBlobAccessGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	uuidGenerator := mock.NewMockUUIDGenerator(ctrl)
	blobAccess := grpcclients.NewCASWithZstdBlobAccess(client, uuidGenerator.Call, 10, 1024)

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
				LowApiVersion:  &semver.SemVer{Major: 2},
				HighApiVersion: &semver.SemVer{Major: 2},
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
				},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
				LowApiVersion:  &semver.SemVer{Major: 2},
				HighApiVersion: &semver.SemVer{Major: 2},
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
				SupportedCompressors: []remoteexecution.Compressor_Value{
					remoteexecution.Compressor_ZSTD,
				},
			},
		}, serverCapabilities)
	})
}
