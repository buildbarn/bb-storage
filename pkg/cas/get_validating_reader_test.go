package cas_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGetValidatingReaderSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	// Blobs that fit in a single chunk are trusted, as their digest
	// is the key under which they are stored.
	chunkBytesReader.EXPECT().Read(ctx, d).Return(data, nil)

	r, err := cas.GetValidatingReader(ctx, chunkBytesReader, chunkMappingFetcher, params, d)
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestGetValidatingReaderSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)

	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "5d41402abc4b2a76b9719d911017c592", 5)
	worldDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "7d793037a0760186574b0282f2f435e7", 5)
	// Digest of "helloworld".
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "fc5e038d38a57032085441e7fe7010b0", 10)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 2}

	mapping, err := chunk.NewMapping([]digest.Digest{helloDigest, worldDigest}, 10, true)
	require.NoError(t, err)
	chunkMappingFetcher.EXPECT().FetchChunkMapping(ctx, d).Return(mapping, nil)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return([]byte("world"), nil),
	)

	r, err := cas.GetValidatingReader(ctx, chunkBytesReader, chunkMappingFetcher, params, d)
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("helloworld"), got)
}

func TestGetValidatingReaderDigestMismatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)

	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "5d41402abc4b2a76b9719d911017c592", 5)
	worldDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "7d793037a0760186574b0282f2f435e7", 5)
	// Digest of "helloWORLD", while the chunks concatenate to
	// "helloworld".
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "23d39d0efc1654475821e6e4601aedb5", 10)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 2}

	mapping, err := chunk.NewMapping([]digest.Digest{helloDigest, worldDigest}, 10, true)
	require.NoError(t, err)
	chunkMappingFetcher.EXPECT().FetchChunkMapping(ctx, d).Return(mapping, nil)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return([]byte("world"), nil),
	)

	r, err := cas.GetValidatingReader(ctx, chunkBytesReader, chunkMappingFetcher, params, d)
	require.NoError(t, err)

	data := make([]byte, 5)
	n, err := r.Read(data)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), data)

	_, err = r.Read(data)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Blob digest mismatch: advertised 3-23d39d0efc1654475821e6e4601aedb5-10-instance, actual 3-fc5e038d38a57032085441e7fe7010b0-10-instance"), err)
}

func TestGetValidatingReaderFetchChunkMappingError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)

	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "23d39d0efc1654475821e6e4601aedb5", 10)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 2}

	chunkMappingFetcher.EXPECT().FetchChunkMapping(ctx, d).Return(chunk.Mapping{}, status.Error(codes.NotFound, "Chunk mapping not found"))

	_, err := cas.GetValidatingReader(ctx, chunkBytesReader, chunkMappingFetcher, params, d)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Could not fetch chunk mapping: Chunk mapping not found"), err)
}
