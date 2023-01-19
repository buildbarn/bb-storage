package readcaching_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readcaching"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadCachingBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobReplicator := mock.NewMockBlobReplicator(ctrl)
	blobAccess := readcaching.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess, blobReplicator)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

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
		// attempt to replicate.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		blobReplicator.EXPECT().ReplicateSingle(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))

		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("FastError", func(t *testing.T) {
		// Read errors on the fast backend should propagate.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk on fire")))

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)
	})

	t.Run("SlowError", func(t *testing.T) {
		// Replication errors should propagate.
		fastBlobAccess.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		blobReplicator.EXPECT().ReplicateSingle(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk on fire")))

		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)
	})
}

func TestReadCachingBlobAccessGetFromComposite(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobReplicator := mock.NewMockBlobReplicator(ctrl)
	blobAccess := readcaching.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess, blobReplicator)
	parentDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "834c514174f3a7d5952dfa68d4b657f3c4cf78b3973dcf2721731c3861559828", 100)
	childDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	slicer := mock.NewMockBlobSlicer(ctrl)

	// We assume that tests for Get() provides coverage for other
	// scenarios.

	t.Run("Slow", func(t *testing.T) {
		fastBlobAccess.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		blobReplicator.EXPECT().ReplicateComposite(ctx, parentDigest, childDigest, slicer).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))

		data, err := blobAccess.GetFromComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})
}

func TestReadCachingBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobReplicator := mock.NewMockBlobReplicator(ctrl)
	blobAccess := readcaching.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess, blobReplicator)
	blobDigest := digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
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

	slowBlobAccess := mock.NewMockBlobAccess(ctrl)
	fastBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobReplicator := mock.NewMockBlobReplicator(ctrl)
	blobAccess := readcaching.NewReadCachingBlobAccess(slowBlobAccess, fastBlobAccess, blobReplicator)
	digests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)).
		Add(digest.MustNewDigest("default", remoteexecution.DigestFunction_SHA256, "82e35a63ceba37e9646434c5dd412ea577147f1e4a41ccde1614253187e3dbf9", 7)).
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
