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

func TestReadCachingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess)
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("Fast", func(t *testing.T) {
		// Provide a blob that can be served by the fast backend
		// immediately.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))

		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("Slow", func(t *testing.T) {
		// The blob is not present in the fast backend. We'll
		// attempt to load it from the slow backend, storing it
		// into the fast backend. We should then retry serving
		// it from the fast backend.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		slowBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		fastBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("FastGetError", func(t *testing.T) {
		// Read errors on the fast backend should propagate.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk on fire")))

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Disk on fire"), err)
	})

	t.Run("SlowGetError", func(t *testing.T) {
		// Read errors on the slow backend should propagate.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		slowBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk on fire")))
		fastBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				_, err := b.ToByteSlice(100)
				require.Equal(t, status.Error(codes.Internal, "Disk on fire"), err)
				return nil
			})

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Disk on fire"), err)
	})

	t.Run("FastPutError", func(t *testing.T) {
		// Write errors on the fast backend should propagate.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		slowBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))
		fastBlobAccess.EXPECT().Put(ctx, blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Disk on fire")
			})

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Disk on fire"), err)
	})
}

func TestReadCachingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess)
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	buffer := buffer.NewValidatedBufferFromByteSlice([]byte("Hello, world"))

	// Write calls should always be forwarded to the slow backend,
	// as the slow backend acts as the source of truth. We should
	// not be writing into the fast backend, as in many cases we
	// don't truly need the blob locally.
	slowBlobAccess.EXPECT().Put(ctx, blobDigest, buffer).Return(nil)

	err := blobAccess.Put(ctx, blobDigest, buffer)
	require.NoError(t, err)
}

func TestReadCachingBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess)
	digests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)).
		Add(digest.MustNewDigest("default", "82e35a63ceba37e9646434c5dd412ea577147f1e4a41ccde1614253187e3dbf9", 7)).
		Build()

	// FindMissing calls go to the slow backend, as that should act
	// as the source of truth. If blobs are missing there, we must
	// upload them. Whether or not they are in the fast backend is
	// irrelevant, as it may be repopulated from the slow backend.
	slowBlobAccess.EXPECT().FindMissing(ctx, digests).Return(digests, nil)

	missing, err := blobAccess.FindMissing(ctx, digests)
	require.NoError(t, err)
	require.Equal(t, digests, missing)
}
