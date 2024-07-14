package blobstore_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestEmptyBlobInjectingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)

	t.Run("NonEmptySuccess", func(t *testing.T) {
		// Requests for non-empty blobs should be forwarded.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("A")))

		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(1)
		require.NoError(t, err)
		require.Equal(t, []byte("A"), data)
	})

	t.Run("NonEmptyFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(
			buffer.NewBufferFromError(
				status.Error(codes.Internal, "Server on fire")))

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(1)
		testutil.RequireEqualStatus(t, err, status.Error(codes.Internal, "Server on fire"))
	})

	t.Run("EmptySuccess", func(t *testing.T) {
		// Requests for the empty blob should be processed directly.
		data, err := blobAccess.Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)).ToByteSlice(0)
		require.NoError(t, err)
		require.Empty(t, data)
	})

	t.Run("EmptyInvalid", func(t *testing.T) {
		// Validation should still be performed on empty blobs.
		_, err := blobAccess.Get(ctx, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 0)).ToByteSlice(0)
		testutil.RequireEqualStatus(t, err, status.Error(codes.InvalidArgument, "Buffer has checksum d41d8cd98f00b204e9800998ecf8427e, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"))
	})
}

func TestEmptyBlobInjectingBlobAccessGetFromComposite(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)
	parentDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, "d0607e3f454f88e2925ef995f549503b7f67541dc7ac0dcf4662265aed7a749f", 1000)
	slicer := mock.NewMockBlobSlicer(ctrl)

	t.Run("NonEmptySuccess", func(t *testing.T) {
		// Requests for non-empty blobs should be forwarded.
		childDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("A")))

		data, err := blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(1)
		require.NoError(t, err)
		require.Equal(t, []byte("A"), data)
	})

	t.Run("NonEmptyFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		childDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(
			buffer.NewBufferFromError(
				status.Error(codes.Internal, "Server on fire")))

		_, err := blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(1)
		testutil.RequireEqualStatus(t, err, status.Error(codes.Internal, "Server on fire"))
	})

	t.Run("EmptySuccess", func(t *testing.T) {
		// Requests for the empty blob should be processed directly.
		data, err := blobAccess.GetFromComposite(ctx, parentDigest, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0), slicer).ToByteSlice(0)
		require.NoError(t, err)
		require.Empty(t, data)
	})

	t.Run("EmptyInvalid", func(t *testing.T) {
		// Validation should still be performed on empty blobs.
		_, err := blobAccess.GetFromComposite(ctx, parentDigest, digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "3e25960a79dbc69b674cd4ec67a72c62", 0), slicer).ToByteSlice(0)
		testutil.RequireEqualStatus(t, err, status.Error(codes.InvalidArgument, "Buffer has checksum d41d8cd98f00b204e9800998ecf8427e, while 3e25960a79dbc69b674cd4ec67a72c62 was expected"))
	})
}

func TestEmptyBlobInjectingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)

	t.Run("NonEmptySuccess", func(t *testing.T) {
		// Requests for non-empty blobs should be forwarded.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(1)
				require.NoError(t, err)
				require.Equal(t, []byte("A"), data)
				return nil
			})

		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				blobDigest,
				buffer.NewValidatedBufferFromByteSlice([]byte("A"))))
	})

	t.Run("NonEmptyFailure", func(t *testing.T) {
		// Errors from the backend should be propagated.
		blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "7fc56270e7a70fa81a5935b72eacbe29", 1)
		baseBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, blobDigest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			blobAccess.Put(
				ctx,
				blobDigest,
				buffer.NewValidatedBufferFromByteSlice([]byte("A"))))
	})

	t.Run("EmptySuccess", func(t *testing.T) {
		// Requests for the empty blob should be processed directly.
		require.NoError(
			t,
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0),
				buffer.NewValidatedBufferFromByteSlice(nil)))
	})

	t.Run("EmptyFailure", func(t *testing.T) {
		// Providing buffers that are in an error state should
		// not cause the error message to be discarded, even
		// when the blob is empty.
		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0),
				buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire"))))
	})
}

func TestEmptyBlobInjectingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewEmptyBlobInjectingBlobAccess(baseBlobAccess)

	unfilteredInputSet := digest.NewSetBuilder().
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
		Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
		Build()
	filteredInputSet := digest.NewSetBuilder().
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
