package blobstore_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestInstanceNameAccessCheckingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	instanceNameMatcher := mock.NewMockInstanceNameMatcher(ctrl)
	blobAccess := blobstore.NewInstanceNameAccessCheckingBlobAccess(baseBlobAccess, instanceNameMatcher.Call)

	// Get calls are forwarded without any forms of processing.
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	baseBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))

	data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)
}

func TestInstanceNameAccessCheckingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	instanceNameMatcher := mock.NewMockInstanceNameMatcher(ctrl)
	blobAccess := blobstore.NewInstanceNameAccessCheckingBlobAccess(baseBlobAccess, instanceNameMatcher.Call)
	helloDigest := digest.MustNewDigest("allowed", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("PermissionDenied", func(t *testing.T) {
		// Requests for non-empty blobs should be forwarded.
		instanceNameMatcher.EXPECT().Call(digest.MustNewInstanceName("denied")).Return(false)

		require.Equal(
			t,
			status.Error(codes.PermissionDenied, "This service does not permit writes for instance name \"denied\""),
			blobAccess.Put(
				ctx,
				digest.MustNewDigest("denied", "7fc56270e7a70fa81a5935b72eacbe29", 1),
				buffer.NewValidatedBufferFromByteSlice([]byte("A"))))
	})

	t.Run("BackendFailure", func(t *testing.T) {
		// Backend failures should be propagated.
		instanceNameMatcher.EXPECT().Call(digest.MustNewInstanceName("allowed")).Return(true)
		baseBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "I/O error")
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "I/O error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("Success", func(t *testing.T) {
		// Successful write against the backend.
		instanceNameMatcher.EXPECT().Call(digest.MustNewInstanceName("allowed")).Return(true)
		baseBlobAccess.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})

		require.NoError(
			t,
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestInstanceNameAccessCheckingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	instanceNameMatcher := mock.NewMockInstanceNameMatcher(ctrl)
	blobAccess := blobstore.NewInstanceNameAccessCheckingBlobAccess(baseBlobAccess, instanceNameMatcher.Call)

	// FindMissing calls are forwarded without any forms of processing.
	digests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)).
		Add(digest.MustNewDigest("default", "82e35a63ceba37e9646434c5dd412ea577147f1e4a41ccde1614253187e3dbf9", 7)).
		Build()
	baseBlobAccess.EXPECT().FindMissing(ctx, digests).Return(digests, nil)

	missing, err := blobAccess.FindMissing(ctx, digests)
	require.NoError(t, err)
	require.Equal(t, digests, missing)
}
