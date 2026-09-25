package blobstore_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestEmptyBlobInjectingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)

	t.Run("NonEmptySuccess", func(t *testing.T) {
		// Requests for non-empty blobs should be forwarded.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)

		expectedChunk := chunk.NewChunk(nil, []byte("A"))
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(expectedChunk, nil)

		chunk, err := blobAccess.Get(ctx, blobDigest)
		require.NoError(t, err)
		data := chunk.GetBytes()
		require.Equal(t, []byte("A"), data)
	})

	t.Run("NonEmptyFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(
			nil, status.Error(codes.Internal, "Server on fire"),
		)

		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t, err, status.Error(codes.Internal, "Server on fire"))
	})

	t.Run("EmptySuccess", func(t *testing.T) {
		// Requests for the empty blob should be processed directly.
		chunk, err := blobAccess.Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0))
		require.NoError(t, err)
		data := chunk.GetBytes()
		require.Empty(t, data)
	})

	t.Run("EmptyInvalid", func(t *testing.T) {
		// Validation should still be performed on empty blobs.
		// Note: The new generic BlobAccess implementations typically leave payload-vs-digest
		// integrity checking to the calling coders or the chunk itself, but if EmptyBlobInjectingBlobAccess
		// still explicitly validates the hash of the requested digest, this test remains relevant.
		_, err := blobAccess.Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 0))
		testutil.RequireEqualStatus(t, err, status.Error(codes.InvalidArgument, "Empty blob has checksum d41d8cd98f00b204e9800998ecf8427e, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"))
	})
}

func TestEmptyBlobInjectingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)

	t.Run("NonEmptySuccess", func(t *testing.T) {
		// Requests for non-empty blobs should be forwarded.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, c *chunk.Chunk) error {
				data := c.GetBytes()
				require.Equal(t, []byte("A"), data)
				return nil
			},
		)

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				blobDigest,
				chunk.NewChunk(nil, []byte("A")),
			),
		)
	})

	t.Run("NonEmptyFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, c *chunk.Chunk) error {
				return status.Error(codes.Internal, "Server on fire")
			},
		)

		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			blobAccess.Put(
				ctx,
				blobDigest,
				chunk.NewChunk(nil, []byte("A")),
			),
		)
	})

	t.Run("EmptySuccess", func(t *testing.T) {
		// Requests for the empty blob should be processed directly.
		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0),
				chunk.NewChunk(nil, nil),
			),
		)
	})

	// The "EmptyFailure" test (which tested passing a buffer initialized with NewBufferFromError)
	// was removed here because with the new generic BlobAccess, `Put` accepts a materialized `*chunk.Chunk`.
	// There is no longer a concept of passing a "delayed error" object into Put.
}

func TestEmptyBlobInjectingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)

	unfilteredInputSet := digest.NewSetBuilder(0).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
		Build()
	filteredInputSet := digest.NewSetBuilder(0).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
		Build()
	outputSet := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7).ToSingletonSet()

	t.Run("Success", func(t *testing.T) {
		// Digests of empty blobs should be filtered from the
		// input set provided to the backend.
		baseBlobAccess.EXPECT().FindMissing(ctx, filteredInputSet).
			Return(outputSet, nil)

		missing, err := blobAccess.FindMissing(ctx, unfilteredInputSet)
		require.NoError(t, err)
		require.Equal(t, outputSet, missing)
	})

	t.Run("Failure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		baseBlobAccess.EXPECT().FindMissing(ctx, filteredInputSet).
			Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, unfilteredInputSet)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})
}
