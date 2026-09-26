package cas_test

import (
	"bytes"
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestContentAddressableStoragePutReaderSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(nil)

	require.NoError(t, cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, d, bytes.NewReader(data)))
}

func TestContentAddressableStoragePutReaderMultipleChunks(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The input is slightly larger than twice the minimum chunk size,
	// so that it decomposes into exactly two chunks.
	data := []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The end.")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "c1df2b2b8aa945f29de62076a8f1e2d9", int64(len(data)))
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	expectedChunkMapping := chunk.Mapping{
		Digests: []digest.Digest{
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "9871f053ed93e778d5090e3dc038815d", 77),
			digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "2044d462a0be5850ee579b02a1ca3b25", 66),
		},
		Offsets:   []uint64{0, 77},
		Validated: true,
	}

	chunkStorage.EXPECT().Put(ctx, expectedChunkMapping.Digests[0], gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), data[:77])
	})).Return(nil)
	chunkStorage.EXPECT().Put(ctx, expectedChunkMapping.Digests[1], gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), data[77:])
	})).Return(nil)
	chunkMappingStorage.EXPECT().Put(ctx, d, expectedChunkMapping).Return(nil)

	require.NoError(t, cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, d, bytes.NewReader(data)))
}

func TestContentAddressableStoragePutReaderRejectsBadDigests(t *testing.T) {
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
				chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
				chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
				chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				err := cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, tc.digest, bytes.NewReader(size.data))
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	}
}

func TestContentAddressableStoragePutReaderEmptyStream(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// An empty stream contains no chunks at all, so neither the Chunk
	// Storage nor the Chunk Mapping Storage may be touched.
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	require.NoError(t, cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, d, bytes.NewReader(nil)))
}

func TestContentAddressableStoragePutReaderPropagatesChunkStorageErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire"))

	err := cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, d, bytes.NewReader(data))
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to save chunk: Disk on fire"), err)
}

func TestContentAddressableStoragePutReaderPropagatesChunkMappingStorageErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The input is slightly larger than twice the minimum chunk size,
	// so that it decomposes into exactly two chunks and a chunk mapping
	// needs to be stored.
	data := []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The end.")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "c1df2b2b8aa945f29de62076a8f1e2d9", int64(len(data)))
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	chunkMappingStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire"))

	err := cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, d, bytes.NewReader(data))
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Could not save chunk mapping for blob: Disk on fire"), err)
}

func TestContentAddressableStoragePutReaderOversizedStream(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The stream contains more data than the digest accounts for. The
	// upload must be rejected as soon as the excess is detected, after
	// the first (and only) chunk has been stored.
	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 3)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().Put(ctx, digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5), gomock.Any()).Return(nil)

	err := cas.PutReader(ctx, zstdPool, chunkStorage, chunkMappingStorage, params, d, bytes.NewReader(data))
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Blob digest mismatch, digest is supposed to be 3 bytes but have already received 5 bytes"), err)
}
