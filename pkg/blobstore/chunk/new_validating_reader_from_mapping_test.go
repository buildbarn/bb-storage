package chunk_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	helloDigest = digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "5d41402abc4b2a76b9719d911017c592", 5)
	worldDigest = digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "7d793037a0760186574b0282f2f435e7", 5)
	// Digest of "helloworld".
	helloWorldDigest = digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "fc5e038d38a57032085441e7fe7010b0", 10)
	// Digest of "helloWORLD", i.e. the digest that a validating
	// reader would compute if the second chunk contained "WORLD".
	helloWORLDDigest = digest.MustNewDigest("test", remoteexecution.DigestFunction_MD5, "23d39d0efc1654475821e6e4601aedb5", 10)
)

func TestValidatingReaderFromMappingSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return([]byte("world"), nil),
	)

	r := chunk.NewValidatingReaderFromMapping(
		ctx,
		[]digest.Digest{helloDigest, worldDigest},
		helloWorldDigest,
		chunkBytesReader)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, []byte("helloworld"), data)
}

func TestValidatingReaderFromMappingDigestMismatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return([]byte("WORLD"), nil),
	)

	r := chunk.NewValidatingReaderFromMapping(
		ctx,
		[]digest.Digest{helloDigest, worldDigest},
		helloWorldDigest,
		chunkBytesReader)

	// The first chunk may be read, as it does not trigger
	// verification.
	data := make([]byte, 5)
	n, err := r.Read(data)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), data)

	// Reading the second chunk must yield an error, as the
	// concatenated contents cannot hash to the advertised digest.
	_, err = r.Read(data)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Blob digest mismatch: advertised 3-fc5e038d38a57032085441e7fe7010b0-10-test, actual 3-23d39d0efc1654475821e6e4601aedb5-10-test"), err)

	// The error must be sticky, as the contents of the final chunk
	// have been revealed to be untrustworthy.
	n, err = r.Read(data)
	require.Equal(t, 0, n)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Blob digest mismatch: advertised 3-fc5e038d38a57032085441e7fe7010b0-10-test, actual 3-23d39d0efc1654475821e6e4601aedb5-10-test"), err)
}

func TestValidatingReaderFromMappingChunkFetchError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	chunkBytesReader := mock.NewMockReader[[]byte](ctrl)
	gomock.InOrder(
		chunkBytesReader.EXPECT().Read(ctx, helloDigest).Return([]byte("hello"), nil),
		chunkBytesReader.EXPECT().Read(ctx, worldDigest).Return(nil, status.Error(codes.NotFound, "Chunk not found")),
	)

	r := chunk.NewValidatingReaderFromMapping(
		ctx,
		[]digest.Digest{helloDigest, worldDigest},
		helloWorldDigest,
		chunkBytesReader)

	data := make([]byte, 5)
	n, err := r.Read(data)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, []byte("hello"), data)

	_, err = r.Read(data)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Failed to fetch chunk at index 1: Chunk not found"), err)
}
