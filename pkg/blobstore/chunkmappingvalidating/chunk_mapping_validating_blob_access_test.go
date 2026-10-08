package chunkmappingvalidating_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunkmappingvalidating"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mustComputeDigest is a test helper to easily generate digests from
// byte slices.
func mustComputeDigest(t *testing.T, digestFunction digest.Function, data []byte) digest.Digest {
	t.Helper()
	generator := digestFunction.NewGenerator(int64(len(data)))
	_, err := generator.Write(data)
	require.NoError(t, err)
	return generator.Sum()
}

var testCDCParams = &remoteexecution.RepMaxCdcParams{
	MinChunkSizeBytes: 1024,
	HorizonSizeBytes:  8 * 1024,
}

func TestChunkMappingValidatingBlobAccessGetServesStoredMapping(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, mock.NewMockReaderPutter(ctrl))

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "deadbeefdeadbeefdeadbeefdeadbeef", 2048)
	chunk1Digest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "abbaabbaabbaabbaabbaabbaabbaabba", 1024)
	chunk2Digest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "cafebabecafebabecafebabecafebabe", 1024)

	storedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, uint64(blobDigest.GetSizeBytes()), false)
	require.NoError(t, err)
	chunkMappingStorage.EXPECT().Get(gomock.Any(), blobDigest).Return(storedChunkMapping, nil)

	chunkMapping, err := validatingCMS.Get(ctx, blobDigest)
	require.NoError(t, err)
	require.Equal(t, []digest.Digest{chunk1Digest, chunk2Digest}, chunkMapping.GetDigests())
}

func TestChunkMappingValidatingBlobAccessGetMissingBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, mock.NewMockReaderPutter(ctrl))

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	blobDigest := mustComputeDigest(t, digestFunction, []byte("Hello, World!"))

	chunkMappingStorage.EXPECT().Get(gomock.Any(), blobDigest).Return(chunk.Mapping{}, status.Error(codes.NotFound, "Blob not found"))

	_, err := validatingCMS.Get(ctx, blobDigest)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkMappingValidatingBlobAccessPutManualSplice(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, readerPutter)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunk1Data := []byte("Hello, ")
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	chunk2Data := []byte("World!")
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)

	expectedFullData := []byte("Hello, World!")
	fullBlobDigest := mustComputeDigest(t, digestFunction, expectedFullData)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), digestFunction.GetInstanceName()).Return(testCDCParams, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.NewSetBuilder(2).Add(chunk1Digest).Add(chunk2Digest).Build()).Return(digest.EmptySet, nil)
	chunkBytesReader.EXPECT().Read(gomock.Any(), chunk1Digest).Return(chunk1Data, nil)
	chunkBytesReader.EXPECT().Read(gomock.Any(), chunk2Digest).Return(chunk2Data, nil)
	readerPutter.EXPECT().PutReader(gomock.Any(), fullBlobDigest, gomock.Any(), testCDCParams).DoAndReturn(
		func(ctx context.Context, d digest.Digest, r io.Reader, params *remoteexecution.RepMaxCdcParams) error {
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, expectedFullData, data)
			return nil
		},
	)

	chunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, uint64(fullBlobDigest.GetSizeBytes()), false)
	require.NoError(t, err)
	err = validatingCMS.Put(ctx, fullBlobDigest, chunkMapping)
	require.NoError(t, err)
}

func TestChunkMappingValidatingBlobAccessPutCanonicalization(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, readerPutter)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	blobData := bytes.Repeat([]byte("testdatafortests"), 250)
	chunk1Data := blobData[:len(blobData)/2]
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Data)
	chunk2Data := blobData[len(blobData)/2:]
	chunk2Digest := mustComputeDigest(t, digestFunction, chunk2Data)

	fullBlobDigest := mustComputeDigest(t, digestFunction, blobData)

	// The stored chunk mapping is recomputed by the ReaderPutter;
	// see pkg/cas/reader_putter_test.go. The validating layer only
	// needs to accept the non-canonical chunks and delegate.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), digestFunction.GetInstanceName()).Return(testCDCParams, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.NewSetBuilder(2).Add(chunk1Digest).Add(chunk2Digest).Build()).Return(digest.EmptySet, nil)
	chunkMappingStorage.EXPECT().Get(gomock.Any(), fullBlobDigest).Return(chunk.Mapping{}, status.Error(codes.NotFound, "Blob not found"))
	chunkBytesReader.EXPECT().Read(gomock.Any(), chunk1Digest).Return(chunk1Data, nil)
	chunkBytesReader.EXPECT().Read(gomock.Any(), chunk2Digest).Return(chunk2Data, nil)
	readerPutter.EXPECT().PutReader(gomock.Any(), fullBlobDigest, gomock.Any(), testCDCParams).DoAndReturn(
		func(ctx context.Context, d digest.Digest, r io.Reader, params *remoteexecution.RepMaxCdcParams) error {
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, blobData, data)
			return nil
		},
	)

	chunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, uint64(fullBlobDigest.GetSizeBytes()), false)
	require.NoError(t, err)
	err = validatingCMS.Put(ctx, fullBlobDigest, chunkMapping)
	require.NoError(t, err)
}

func TestChunkMappingValidatingBlobAccessPutMissingChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, mock.NewMockReaderPutter(ctrl))

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunk1Bytes := []byte("A ")
	chunk1Digest := mustComputeDigest(t, digestFunction, chunk1Bytes)
	chunk2Digest := mustComputeDigest(t, digestFunction, []byte("ghost"))
	blobDigest := mustComputeDigest(t, digestFunction, []byte("A ghost"))

	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), digestFunction.GetInstanceName()).Return(testCDCParams, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.NewSetBuilder(2).Add(chunk1Digest).Add(chunk2Digest).Build()).Return(chunk2Digest.ToSingletonSet(), nil)

	chunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, uint64(blobDigest.GetSizeBytes()), false)
	require.NoError(t, err)
	err = validatingCMS.Put(ctx, blobDigest, chunkMapping)
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestChunkMappingValidatingBlobAccessPutRepeatedChunks(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, readerPutter)

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)

	chunkAData := []byte("A")
	digestA := mustComputeDigest(t, digestFunction, chunkAData)
	chunkBData := []byte("B")
	digestB := mustComputeDigest(t, digestFunction, chunkBData)

	// Repeated chunks may only be checked for existence once.
	expectedData := []byte("AABA")
	expectedDigest := mustComputeDigest(t, digestFunction, expectedData)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), digestFunction.GetInstanceName()).Return(testCDCParams, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.NewSetBuilder(2).Add(digestA).Add(digestB).Build()).Return(digest.EmptySet, nil)
	chunkBytesReader.EXPECT().Read(gomock.Any(), digestA).Return(chunkAData, nil).Times(3)
	chunkBytesReader.EXPECT().Read(gomock.Any(), digestB).Return(chunkBData, nil)
	readerPutter.EXPECT().PutReader(gomock.Any(), expectedDigest, gomock.Any(), testCDCParams).DoAndReturn(
		func(ctx context.Context, d digest.Digest, r io.Reader, params *remoteexecution.RepMaxCdcParams) error {
			data, err := io.ReadAll(r)
			require.NoError(t, err)
			require.Equal(t, expectedData, data)
			return nil
		},
	)

	chunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{digestA, digestA, digestB, digestA}, uint64(expectedDigest.GetSizeBytes()), false)
	require.NoError(t, err)
	err = validatingCMS.Put(ctx, expectedDigest, chunkMapping)
	require.NoError(t, err)
}

func TestChunkMappingValidatingBlobAccessPutValidatedPassthrough(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	validatingCMS := chunkmappingvalidating.NewChunkMappingValidatingBlobAccess(chunkMappingStorage, chunkStorage, cdcParametersFetcher, chunkBytesReader, mock.NewMockReaderPutter(ctrl))

	digestFunction := digest.MustNewFunction("instance", remoteexecution.DigestFunction_SHA256)
	chunk1Digest := mustComputeDigest(t, digestFunction, []byte("Hello, "))
	chunk2Digest := mustComputeDigest(t, digestFunction, []byte("World!"))
	blobDigest := mustComputeDigest(t, digestFunction, []byte("Hello, World!"))

	// Validated chunk mappings are pushed directly to the downstream
	// blob store without any verification.
	validatedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, uint64(blobDigest.GetSizeBytes()), true)
	require.NoError(t, err)
	chunkMappingStorage.EXPECT().Put(ctx, blobDigest, validatedChunkMapping).Return(nil)

	err = validatingCMS.Put(ctx, blobDigest, validatedChunkMapping)
	require.NoError(t, err)
}
