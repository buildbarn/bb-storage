package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunklist"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestContentAddressableStoragePutBytesSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*buffer.Chunk](ctrl)
	chunkListStorage := mock.NewMockBlobAccess[chunklist.ChunkList](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().Put(ctx, d, buffer.NewChunk(zstdPool, data)).Return(nil)

	require.NoError(t, cas.PutBytes(ctx, zstdPool, chunkStorage, chunkListStorage, params, d, data))
}

func TestContentAddressableStoragePutBytesMultipleChunks(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*buffer.Chunk](ctrl)
	chunkListStorage := mock.NewMockBlobAccess[chunklist.ChunkList](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The input is slightly larger than twice the minimum chunk size,
	// so that it decomposes into exactly two chunks.
	data := []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The end.")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "c1df2b2b8aa945f29de62076a8f1e2d9", int64(len(data)))
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	expectedChunkList := chunklist.ChunkList{
		Digests: []digest.Digest{
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "9871f053ed93e778d5090e3dc038815d", 77),
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "2044d462a0be5850ee579b02a1ca3b25", 66),
		},
		Offsets:   []uint64{0, 77},
		Validated: true,
	}

	chunkStorage.EXPECT().Put(ctx, expectedChunkList.Digests[0], buffer.NewChunk(zstdPool, data[:77])).Return(nil)
	chunkStorage.EXPECT().Put(ctx, expectedChunkList.Digests[1], buffer.NewChunk(zstdPool, data[77:])).Return(nil)
	chunkListStorage.EXPECT().Put(ctx, d, expectedChunkList).Return(nil)

	require.NoError(t, cas.PutBytes(ctx, zstdPool, chunkStorage, chunkListStorage, params, d, data))
}

func TestContentAddressableStoragePutBytesRejectsBadDigests(t *testing.T) {
	zstdPool := zstd.NewPoolFromConfiguration(nil)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}
	otherHash := "eab353922642ccc0f6fd34c5e0c57888"

	for _, size := range []struct {
		name     string
		data     []byte
		dataHash string
	}{
		{
			// Shorter than twice the minimum chunk size, so that all
			// digests are stored as a single chunk.
			name:     "SingleChunk",
			data:     []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick"),
			dataHash: "2fc58fafbfb718e48bcdacdb8474ec0f",
		},
		{
			// Slightly larger than twice the minimum chunk size, so
			// that all digests take the whole blob verification path.
			name:     "MultiChunk",
			data:     []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The end."),
			dataHash: "c1df2b2b8aa945f29de62076a8f1e2d9",
		},
	} {
		oversizeDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, size.dataHash, int64(len(size.data)+8))
		undersizeDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, size.dataHash, int64(len(size.data)-8))
		otherDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, otherHash, int64(len(size.data)))

		tests := []struct {
			name   string
			digest digest.Digest
		}{
			{name: "DigestOversizeWithCorrectHash", digest: oversizeDigest},
			{name: "DigestUndersizeWithCorrectHash", digest: undersizeDigest},
			{name: "HashMismatch", digest: otherDigest},
		}
		for _, tc := range tests {
			t.Run(size.name+"/"+tc.name, func(t *testing.T) {
				ctrl, ctx := gomock.WithContext(context.Background(), t)
				chunkStorage := mock.NewMockBlobAccess[*buffer.Chunk](ctrl)
				chunkListStorage := mock.NewMockBlobAccess[chunklist.ChunkList](ctrl)
				chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				err := cas.PutBytes(ctx, zstdPool, chunkStorage, chunkListStorage, params, tc.digest, size.data)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	}
}
