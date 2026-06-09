package cas_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"testing"
	"testing/iotest"

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

func TestReaderPutterSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().FindMissing(ctx, d.ToSingletonSet()).Return(d.ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(nil)

	require.NoError(t, cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(data), params))
}

func TestReaderPutterMultipleChunks(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// The input is slightly larger than twice the minimum chunk size,
	// so that it decomposes into exactly two chunks.
	data := []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The end.")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "c1df2b2b8aa945f29de62076a8f1e2d9", int64(len(data)))
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	expectedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{
		digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "9871f053ed93e778d5090e3dc038815d", 77),
		digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "2044d462a0be5850ee579b02a1ca3b25", 66),
	}, 143, true)
	require.NoError(t, err)

	chunkStorage.EXPECT().FindMissing(ctx, expectedChunkMapping.GetDigestAtIndex(0).ToSingletonSet()).Return(expectedChunkMapping.GetDigestAtIndex(0).ToSingletonSet(), nil)
	chunkStorage.EXPECT().FindMissing(ctx, expectedChunkMapping.GetDigestAtIndex(1).ToSingletonSet()).Return(expectedChunkMapping.GetDigestAtIndex(1).ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(ctx, expectedChunkMapping.GetDigestAtIndex(0), gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), data[:77])
	})).Return(nil)
	chunkStorage.EXPECT().Put(ctx, expectedChunkMapping.GetDigestAtIndex(1), gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), data[77:])
	})).Return(nil)
	chunkMappingStorage.EXPECT().Put(ctx, d, expectedChunkMapping).Return(nil)

	require.NoError(t, cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(data), params))
}

func TestReaderPutterRejectsBadDigests(t *testing.T) {
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
				chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, digests digest.Set) (digest.Set, error) {
					// Report all requested digests as missing.
					return digests, nil
				}).AnyTimes()
				chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, tc.digest, bytes.NewReader(size.data), params)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	}
}

func TestReaderPutterEmptyStream(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	// An empty stream contains no chunks at all, so neither the Chunk
	// Storage nor the Chunk Mapping Storage may be touched.
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	require.NoError(t, cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(nil), params))
}

func TestReaderPutterPropagatesChunkStorageErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().FindMissing(ctx, d.ToSingletonSet()).Return(d.ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire"))

	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(data), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to save chunk: Disk on fire"), err)
}

func TestReaderPutterPropagatesChunkMappingStorageErrors(t *testing.T) {
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

	chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, digests digest.Set) (digest.Set, error) {
		// Report all requested digests as missing.
		return digests, nil
	}).Times(2)
	chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(2)
	chunkMappingStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire"))

	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(data), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Could not save chunk mapping for blob: Disk on fire"), err)
}

func TestReaderPutterPropagatesStreamErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, iotest.ErrReader(errors.New("Write stream failed")), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Unknown, "Failed to chunk write stream: Write stream failed"), err)
}

func TestReaderPutterOversizedStream(t *testing.T) {
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

	chunkStorage.EXPECT().FindMissing(ctx, digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet()).Return(digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(ctx, digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5), gomock.Any()).Return(nil)

	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(data), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.InvalidArgument, "Blob digest mismatch, digest is supposed to be 3 bytes but have already received 5 bytes"), err)
}

const (
	fuzzMinChunkSize          = 256 << 10 // 256 KiB
	fuzzMaxChunkSize          = 2*fuzzMinChunkSize - 1
	fuzzHorizonLookaheadBytes = 8 * fuzzMinChunkSize
)

func FuzzReaderPutter(f *testing.F) {
	for i := range 20 {
		// Fuzz test i+1 MB of data with seed i.
		f.Add((i+1)<<20, int64(i))
	}
	f.Fuzz(func(t *testing.T, dataSizeBytes int, seed int64) {
		require := require.New(t)
		rng := rand.New(rand.NewSource(seed))
		originalData := make([]byte, dataSizeBytes)
		rng.Read(originalData)

		digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256)
		wholeGen := digestFunction.NewGenerator(int64(dataSizeBytes))
		wholeGen.Write(originalData)
		d := wholeGen.Sum()
		params := &remoteexecution.RepMaxCdcParams{
			MinChunkSizeBytes: fuzzMinChunkSize,
			HorizonSizeBytes:  fuzzHorizonLookaheadBytes,
		}

		ctrl, ctx := gomock.WithContext(context.Background(), t)
		chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
		chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
		zstdPool := zstd.NewPoolFromConfiguration(nil)

		type storedChunk struct {
			digest digest.Digest
			data   []byte
		}
		chunks := make([]storedChunk, 0)
		chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, digests digest.Set) (digest.Set, error) {
				// Report all requested digests as missing, so that
				// every chunk is stored and can be recorded.
				return digests, nil
			},
		).AnyTimes()
		chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, d digest.Digest, c *chunk.Chunk) error {
				chunks = append(chunks, storedChunk{digest: d, data: c.GetBytes()})
				return nil
			},
		).AnyTimes()
		chunkMappingStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

		require.NoError(cas.NewReaderPutter(chunkStorage, chunkMappingStorage, zstdPool).PutReader(ctx, d, bytes.NewReader(originalData), params))

		composedData := make([]byte, 0, dataSizeBytes)
		for i, c := range chunks {
			chunkGen := digestFunction.NewGenerator(int64(len(c.data)))
			chunkGen.Write(c.data)
			require.Equal(chunkGen.Sum(), c.digest, "Digest mismatch for chunk %d.", i)
			composedData = append(composedData, c.data...)
		}
		require.Equal(originalData, composedData)

		numberOfChunks := len(chunks)
		minNumberOfChunks := dataSizeBytes / fuzzMaxChunkSize
		require.GreaterOrEqual(numberOfChunks, minNumberOfChunks, "Produced fewer chunks than should be possible.")

		maxNumberOfChunks := dataSizeBytes / fuzzMinChunkSize
		require.LessOrEqual(numberOfChunks, maxNumberOfChunks, "Produced more chunks than should be possible.")
	})
}
