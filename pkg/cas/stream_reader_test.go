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

func TestStreamReaderReadSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	data := []byte("Hello")
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 64, HorizonSizeBytes: 128}

	cdcParametersFetcher.EXPECT().FetchCDCParameters(ctx, d.GetInstanceName()).Return(params, nil)
	// Blobs that fit in a single chunk are trusted, as their digest
	// is the key under which they are stored.
	chunkBytesReader.EXPECT().Read(ctx, d).Return(data, nil)

	streamReader := cas.NewStorageBackedStreamReader(chunkBytesReader, chunkMappingFetcher, cdcParametersFetcher)
	r, err := streamReader.ReadStream(ctx, d)
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestStreamReaderReadSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "5d41402abc4b2a76b9719d911017c592", 5)
	worldDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "7d793037a0760186574b0282f2f435e7", 5)
	// Digest of "helloworld".
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "fc5e038d38a57032085441e7fe7010b0", 10)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 2}

	cdcParametersFetcher.EXPECT().FetchCDCParameters(ctx, d.GetInstanceName()).Return(params, nil)
	mapping, err := chunk.NewMapping([]digest.Digest{helloDigest, worldDigest}, 10, true)
	require.NoError(t, err)
	chunkMappingFetcher.EXPECT().FetchChunkMapping(ctx, d).Return(mapping, nil)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return([]byte("world"), nil),
	)

	streamReader := cas.NewStorageBackedStreamReader(chunkBytesReader, chunkMappingFetcher, cdcParametersFetcher)
	r, err := streamReader.ReadStream(ctx, d)
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("helloworld"), got)
}

func TestStreamReaderReadValidatesDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	helloDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "5d41402abc4b2a76b9719d911017c592", 5)
	worldDigest := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "7d793037a0760186574b0282f2f435e7", 5)
	// Digest of "helloWORLD", while the chunks concatenate to
	// "helloworld".
	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "23d39d0efc1654475821e6e4601aedb5", 10)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 2}

	cdcParametersFetcher.EXPECT().FetchCDCParameters(ctx, d.GetInstanceName()).Return(params, nil)
	mapping, err := chunk.NewMapping([]digest.Digest{helloDigest, worldDigest}, 10, true)
	require.NoError(t, err)
	chunkMappingFetcher.EXPECT().FetchChunkMapping(ctx, d).Return(mapping, nil)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return([]byte("world"), nil),
	)

	streamReader := cas.NewStorageBackedStreamReader(chunkBytesReader, chunkMappingFetcher, cdcParametersFetcher)
	r, err := streamReader.ReadStream(ctx, d)
	require.NoError(t, err)

	data := make([]byte, 5)
	n, err := r.Read(data)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), data)

	// The stream must fail to validate once the final chunk is
	// fetched, as the chunks do not concatenate to the digest.
	_, err = r.Read(data)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Blob digest mismatch, advertised 3-23d39d0efc1654475821e6e4601aedb5-10-instance, actual 3-fc5e038d38a57032085441e7fe7010b0-10-instance"), err)
}

func TestStreamReaderReadFetchCDCParametersError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	chunkMappingFetcher := mock.NewMockMappingFetcher(ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	d := digest.MustNewDigest("instance", remoteexecution.DigestFunction_MD5, "23d39d0efc1654475821e6e4601aedb5", 10)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(ctx, d.GetInstanceName()).Return(nil, status.Error(codes.NotFound, "CDC parameters not found"))

	streamReader := cas.NewStorageBackedStreamReader(chunkBytesReader, chunkMappingFetcher, cdcParametersFetcher)
	_, err := streamReader.ReadStream(ctx, d)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Could not fetch CDC parameters: CDC parameters not found"), err)
}
