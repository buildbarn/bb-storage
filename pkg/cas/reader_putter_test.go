package cas_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func newReaderPutter(chunkStorage *mock.MockBlobAccess[*chunk.Chunk], chunkMappingStorage *mock.MockBlobAccess[chunk.Mapping]) cas.ReaderPutter {
	return cas.NewReaderPutter(chunkStorage, chunkMappingStorage, semaphore.NewWeighted(cas.RecommendedMaximumBufferedChunks))
}

// smallChunkParams make blobs of a few KiB decompose into many chunks.
var smallChunkParams = &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

func randomBlob(sizeBytes int, seed int64) ([]byte, digest.Digest) {
	data := make([]byte, sizeBytes)
	rand.New(rand.NewSource(seed)).Read(data)
	generator := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256).NewGenerator(int64(sizeBytes))
	generator.Write(data)
	return data, generator.Sum()
}

func TestReaderPutterSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().FindMissing(ctx, d.ToSingletonSet()).Return(d.ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(nil)

	require.NoError(t, newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, bytes.NewReader(data), params))
}

func TestReaderPutterMultipleChunks(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

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

	chunkStorage.EXPECT().FindMissing(gomock.Any(), expectedChunkMapping.GetDigestAtIndex(d.GetInstanceName(), 0).ToSingletonSet()).Return(expectedChunkMapping.GetDigestAtIndex(d.GetInstanceName(), 0).ToSingletonSet(), nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), expectedChunkMapping.GetDigestAtIndex(d.GetInstanceName(), 1).ToSingletonSet()).Return(expectedChunkMapping.GetDigestAtIndex(d.GetInstanceName(), 1).ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(gomock.Any(), expectedChunkMapping.GetDigestAtIndex(d.GetInstanceName(), 0), gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), data[:77])
	})).Return(nil)
	chunkStorage.EXPECT().Put(gomock.Any(), expectedChunkMapping.GetDigestAtIndex(d.GetInstanceName(), 1), gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(chunk.GetBytes(), data[77:])
	})).Return(nil)
	chunkMappingStorage.EXPECT().Put(ctx, d, expectedChunkMapping).Return(nil)

	require.NoError(t, newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, bytes.NewReader(data), params))
}

func TestReaderPutterRejectsBadDigests(t *testing.T) {
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}
	otherHash := "eab353922642ccc0f6fd34c5e0c57888"

	for _, size := range []struct {
		name     string
		data     []byte
		dataHash string
	}{
		{
			name:     "SingleChunk",
			data:     []byte("The quick brown fox jumps over the lazy dog. The quick brown fox jumps over the lazy dog. The quick"),
			dataHash: "2fc58fafbfb718e48bcdacdb8474ec0f",
		},
		{
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
				chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).Return(digest.EmptySet, nil).AnyTimes()
				chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
				err := newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, tc.digest, bytes.NewReader(size.data), params)
				require.Equal(t, codes.InvalidArgument, status.Code(err))
			})
		}
	}
}

func TestReaderPutterEmptyStream(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	// An empty stream contains no chunks at all, so neither the Chunk
	// Storage nor the Chunk Mapping Storage may be touched.
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	require.NoError(t, newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, bytes.NewReader(nil), params))
}

func TestReaderPutterPropagatesChunkStorageErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	chunkStorage.EXPECT().FindMissing(ctx, d.ToSingletonSet()).Return(d.ToSingletonSet(), nil)
	chunkStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire"))

	err := newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, bytes.NewReader(data), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to save chunk: Disk on fire"), err)
}

func TestReaderPutterPropagatesChunkMappingStorageErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

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

	err := newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, bytes.NewReader(data), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Could not save chunk mapping for blob: Disk on fire"), err)
}

func TestReaderPutterPropagatesStreamErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	err := newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, iotest.ErrReader(errors.New("Write stream failed")), params)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Unknown, "Failed to read write stream: Write stream failed"), err)
}

const (
	fuzzMinChunkSize          = 256 << 10 // 256 KiB
	fuzzMaxChunkSize          = 2*fuzzMinChunkSize - 1
	fuzzHorizonLookaheadBytes = 8 * fuzzMinChunkSize
	fuzzMaxDataSizeBytes      = 20 << 20 // 20 MiB
)

func FuzzReaderPutter(f *testing.F) {
	for i := range 20 {
		// Fuzz test i+1 MB of data with seed i.
		f.Add((i+1)<<20, int64(i))
	}
	f.Fuzz(func(t *testing.T, dataSizeBytes int, seed int64) {
		// The fuzzer is free to generate any int, including
		// negative and absurdly large ones. Only fuzz data sizes
		// that we are willing to allocate.
		if dataSizeBytes < 0 || dataSizeBytes > fuzzMaxDataSizeBytes {
			return
		}
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

		var lock sync.Mutex
		storedChunks := map[digest.Digest]*chunk.Chunk{}
		var storedMapping *chunk.Mapping
		chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, digests digest.Set) (digest.Set, error) {
				// Report all requested digests as missing, so that
				// every chunk is stored and can be recorded.
				return digests, nil
			},
		).AnyTimes()
		chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(ctx context.Context, d digest.Digest, c *chunk.Chunk) error {
				lock.Lock()
				defer lock.Unlock()
				storedChunks[d] = c
				return nil
			},
		).AnyTimes()
		chunkMappingStorage.EXPECT().Put(gomock.Any(), d, gomock.Any()).DoAndReturn(
			func(ctx context.Context, d digest.Digest, m chunk.Mapping) error {
				storedMapping = &m
				return nil
			},
		).MaxTimes(1)

		require.NoError(newReaderPutter(chunkStorage, chunkMappingStorage).PutReader(ctx, d, bytes.NewReader(originalData), params))

		var chunkDigests []digest.Digest
		if storedMapping != nil {
			chunkDigests = storedMapping.GetDigests(d.GetInstanceName())
		} else {
			for chunkDigest := range storedChunks {
				chunkDigests = append(chunkDigests, chunkDigest)
			}
			require.LessOrEqual(len(chunkDigests), 1, "Stored multiple chunks without a chunk mapping.")
		}
		composedData := make([]byte, 0, dataSizeBytes)
		for i, chunkDigest := range chunkDigests {
			c, ok := storedChunks[chunkDigest]
			require.True(ok, "Chunk %d was not stored.", i)
			require.Equal(c.ComputeDigest(digestFunction), chunkDigest, "Digest mismatch for chunk %d.", i)
			composedData = append(composedData, c.GetBytes()...)
		}
		require.Equal(originalData, composedData)

		numberOfChunks := len(chunkDigests)
		minNumberOfChunks := dataSizeBytes / fuzzMaxChunkSize
		require.GreaterOrEqual(numberOfChunks, minNumberOfChunks, "Produced fewer chunks than should be possible.")

		maxNumberOfChunks := dataSizeBytes / fuzzMinChunkSize
		require.LessOrEqual(numberOfChunks, maxNumberOfChunks, "Produced more chunks than should be possible.")
	})
}

func TestReaderPutterStoresChunksConcurrently(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	data, d := randomBlob(16<<10, 1)

	// Put() only returns once four calls run at the same time.
	const concurrency = 4
	var started atomic.Int64
	allStarted := make(chan struct{})
	chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, digests digest.Set) (digest.Set, error) {
			return digests, nil
		},
	).AnyTimes()
	chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, chunkDigest digest.Digest, c *chunk.Chunk) error {
			if started.Add(1) == concurrency {
				close(allStarted)
			}
			select {
			case <-allStarted:
				return nil
			case <-time.After(10 * time.Second):
				return status.Error(codes.DeadlineExceeded, "Chunks are not stored concurrently")
			}
		},
	).AnyTimes()
	chunkMappingStorage.EXPECT().Put(ctx, d, gomock.Any()).Return(nil)

	require.NoError(t, cas.NewReaderPutter(chunkStorage, chunkMappingStorage, semaphore.NewWeighted(concurrency)).PutReader(ctx, d, bytes.NewReader(data), smallChunkParams))
}

func TestReaderPutterMultipleChunksPropagatesChunkStorageErrors(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	data, d := randomBlob(16<<10, 2)

	chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, digests digest.Set) (digest.Set, error) {
			return digests, nil
		},
	).AnyTimes()
	chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(status.Error(codes.Internal, "Disk on fire")).MinTimes(1)

	chunkSemaphore := semaphore.NewWeighted(4)
	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, chunkSemaphore).PutReader(ctx, d, bytes.NewReader(data), smallChunkParams)
	testutil.RequirePrefixedStatus(t, status.Error(codes.Internal, "Failed to save chunk: Disk on fire"), err)
	require.True(t, chunkSemaphore.TryAcquire(4))
}

func TestReaderPutterReleasesSemaphoreOnBadDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	// The stream is larger than the digest.
	data, _ := randomBlob(16<<10, 3)
	_, d := randomBlob(8<<10, 3)

	chunkStorage.EXPECT().FindMissing(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, digests digest.Set) (digest.Set, error) {
			return digests, nil
		},
	).AnyTimes()
	chunkStorage.EXPECT().Put(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	chunkSemaphore := semaphore.NewWeighted(4)
	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, chunkSemaphore).PutReader(ctx, d, bytes.NewReader(data), smallChunkParams)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.True(t, chunkSemaphore.TryAcquire(4))
}

func TestReaderPutterCanceledWhileWaitingForSemaphore(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)

	data, d := randomBlob(16<<10, 4)

	// Another call holds all units.
	chunkSemaphore := semaphore.NewWeighted(1)
	require.True(t, chunkSemaphore.TryAcquire(1))
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	err := cas.NewReaderPutter(chunkStorage, chunkMappingStorage, chunkSemaphore).PutReader(ctx, d, bytes.NewReader(data), smallChunkParams)
	require.Equal(t, codes.Canceled, status.Code(err))
}
