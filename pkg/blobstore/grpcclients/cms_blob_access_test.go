package grpcclients_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

func TestCMSBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCMSBlobAccess(client, 1<<20)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	chunkDigest1 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "2b58e59caab73fafd9ae2f465c60db5b", 3)
	chunkDigest2 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8d793f38a50f6f7403f08de9d1c9a2a7", 2)

	expectGetChunkMappingStream := func(responses []*remoteexecution.GetChunkMappingResponse) {
		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(
			gomock.Any(),
			gomock.Any(),
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetChunkMapping",
			gomock.Any(),
		).Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &remoteexecution.GetChunkMappingRequest{
			InstanceName:     "hello",
			BlobDigest:       blobDigest.GetProto(),
			DigestFunction:   remoteexecution.DigestFunction_MD5,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}))
		clientStream.EXPECT().CloseSend()
		// Responses are consumed in order. Tests may abort early, so
		// trailing responses are optional.
		i := 0
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			if i < len(responses) {
				proto.Merge(m.(proto.Message), responses[i])
				i++
				return nil
			}
			return io.EOF
		}).AnyTimes()
	}

	t.Run("Success", func(t *testing.T) {
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{
				ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
				ChunkDigests:     []*remoteexecution.Digest{chunkDigest1.GetProto()},
			},
			{
				// Chunking function is only required in the first
				// response; subsequent responses may omit it.
				ChunkDigests: []*remoteexecution.Digest{chunkDigest2.GetProto()},
			},
		})
		chunkMapping, err := blobAccess.Get(ctx, blobDigest)
		require.NoError(t, err)
		require.Equal(t, chunk.Mapping{
			Digests: []digest.Digest{chunkDigest1, chunkDigest2},
			Offsets: []uint64{0, 3},
		}, chunkMapping)
	})

	t.Run("SuccessSingletonList", func(t *testing.T) {
		// The upstream server reports the blob itself as the single
		// chunk of its mapping. Such blobs have no chunk mapping in
		// storage, so this maps to NotFound.
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{
				ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
				ChunkDigests:     []*remoteexecution.Digest{blobDigest.GetProto()},
			},
		})
		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.NotFound, "Blob has no chunk mapping in storage"),
			err)
	})

	t.Run("ChunkingFunctionMismatch", func(t *testing.T) {
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{ChunkingFunction: remoteexecution.ChunkingFunction_FAST_CDC_2020},
		})
		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Server responded with unsupported chunking function FAST_CDC_2020"),
			err)
	})

	t.Run("ChunkDigestInvalid", func(t *testing.T) {
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{
				ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
				ChunkDigests: []*remoteexecution.Digest{{
					Hash:      "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
					SizeBytes: 3,
				}},
			},
		})
		_, err := blobAccess.Get(ctx, blobDigest)
		require.Error(t, err)
	})

	t.Run("SizeSumMismatch", func(t *testing.T) {
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{
				ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
				ChunkDigests: []*remoteexecution.Digest{
					chunkDigest1.GetProto(),
					chunkDigest1.GetProto(),
				},
			},
		})
		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Chunk mapping does not compose to blob"),
			err)
	})

	t.Run("EmptyListForNonEmptyBlob", func(t *testing.T) {
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC},
		})
		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Chunk mapping does not compose to blob"),
			err)
	})

	t.Run("SingletonListDigestMismatch", func(t *testing.T) {
		expectGetChunkMappingStream([]*remoteexecution.GetChunkMappingResponse{
			{
				ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
				ChunkDigests:     []*remoteexecution.Digest{chunkDigest1.GetProto()},
			},
		})
		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Chunk mapping does not compose to blob"),
			err)
	})
}

func TestCMSBlobAccessGetEmptyBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCMSBlobAccess(client, 1<<20)

	emptyBlobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)

	t.Run("EmptyListForEmptyBlob", func(t *testing.T) {
		// The empty blob has no chunk mapping in storage, so an empty
		// mapping composes it and maps to NotFound.
		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(
			gomock.Any(),
			gomock.Any(),
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetChunkMapping",
			gomock.Any(),
		).Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &remoteexecution.GetChunkMappingRequest{
			InstanceName:     "hello",
			BlobDigest:       emptyBlobDigest.GetProto(),
			DigestFunction:   remoteexecution.DigestFunction_MD5,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}))
		clientStream.EXPECT().CloseSend()
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			proto.Merge(m.(proto.Message), &remoteexecution.GetChunkMappingResponse{
				ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			})
			return nil
		})
		clientStream.EXPECT().RecvMsg(gomock.Any()).Return(io.EOF)

		_, err := blobAccess.Get(ctx, emptyBlobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.NotFound, "Blob has no chunk mapping in storage"),
			err)
	})
}

func TestCMSBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCMSBlobAccess(client, 1<<20)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	chunkDigest1 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "2b58e59caab73fafd9ae2f465c60db5b", 3)
	chunkDigest2 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8d793f38a50f6f7403f08de9d1c9a2a7", 2)

	t.Run("Success", func(t *testing.T) {
		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(
			gomock.Any(),
			gomock.Any(),
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/RegisterChunkMapping",
			gomock.Any(),
		).Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &remoteexecution.RegisterChunkMappingRequest{
			InstanceName:     "hello",
			BlobDigest:       blobDigest.GetProto(),
			DigestFunction:   remoteexecution.DigestFunction_MD5,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			ChunkDigests: []*remoteexecution.Digest{
				chunkDigest1.GetProto(),
				chunkDigest2.GetProto(),
			},
		}))
		clientStream.EXPECT().CloseSend()
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			proto.Merge(m.(proto.Message), &remoteexecution.RegisterChunkMappingResponse{BlobDigest: blobDigest.GetProto()})
			return nil
		})

		err := blobAccess.Put(ctx, blobDigest, chunk.Mapping{
			Digests: []digest.Digest{chunkDigest1, chunkDigest2},
			Offsets: []uint64{0, 3},
		})
		require.NoError(t, err)
	})

	t.Run("EmptyListForEmptyBlob", func(t *testing.T) {
		emptyBlobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)

		clientStream := mock.NewMockClientStream(ctrl)
		client.EXPECT().NewStream(
			gomock.Any(),
			gomock.Any(),
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/RegisterChunkMapping",
			gomock.Any(),
		).Return(clientStream, nil)
		clientStream.EXPECT().SendMsg(testutil.EqProto(t, &remoteexecution.RegisterChunkMappingRequest{
			InstanceName:     "hello",
			BlobDigest:       emptyBlobDigest.GetProto(),
			DigestFunction:   remoteexecution.DigestFunction_MD5,
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
			ChunkDigests:     []*remoteexecution.Digest{},
		}))
		clientStream.EXPECT().CloseSend()
		clientStream.EXPECT().RecvMsg(gomock.Any()).DoAndReturn(func(m interface{}) error {
			proto.Merge(m.(proto.Message), &remoteexecution.RegisterChunkMappingResponse{BlobDigest: emptyBlobDigest.GetProto()})
			return nil
		})

		err := blobAccess.Put(ctx, emptyBlobDigest, chunk.Mapping{})
		require.NoError(t, err)
	})
}
