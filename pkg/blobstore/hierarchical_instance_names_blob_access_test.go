package blobstore_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHierarchicalInstanceNamesBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewHierarchicalInstanceNamesBlobAccess(baseBlobAccess)

	helloDigest1 := digest.MustNewDigest("a/b", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigest2 := digest.MustNewDigest("a", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigest3 := digest.MustNewDigest("", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Failure", func(t *testing.T) {
		// Errors from backends should be propagated. The
		// instance name should be prepended to the error
		// message, to disambiguate.
		gomock.InOrder(
			baseBlobAccess.EXPECT().Get(ctx, helloDigest1).
				Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))),
			baseBlobAccess.EXPECT().Get(ctx, helloDigest2).
				Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk on fire"))))

		_, err := blobAccess.Get(ctx, helloDigest1).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Instance name \"a\": Disk on fire"), err)
	})

	t.Run("NotFound", func(t *testing.T) {
		// NotFound errors should cause requests to be retried
		// against parent instance names.
		gomock.InOrder(
			baseBlobAccess.EXPECT().Get(ctx, helloDigest1).
				Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))),
			baseBlobAccess.EXPECT().Get(ctx, helloDigest2).
				Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))),
			baseBlobAccess.EXPECT().Get(ctx, helloDigest3).
				Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))))

		_, err := blobAccess.Get(ctx, helloDigest1).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), err)
	})

	t.Run("Success", func(t *testing.T) {
		gomock.InOrder(
			baseBlobAccess.EXPECT().Get(ctx, helloDigest1).
				Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found"))),
			baseBlobAccess.EXPECT().Get(ctx, helloDigest2).
				Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))

		data, err := blobAccess.Get(ctx, helloDigest1).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestHierarchicalInstanceNamesBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := mock.NewMockBlobAccess(ctrl)
	blobAccess := blobstore.NewHierarchicalInstanceNamesBlobAccess(baseBlobAccess)

	t.Run("InitialFailure", func(t *testing.T) {
		// Errors that occur both during the initial call to
		// FindMissing() and successive ones should be propagated.
		digests := digest.NewSetBuilder().
			Add(digest.MustNewDigest("a", "00000000000000000000000000000001", 1)).
			Add(digest.MustNewDigest("b", "00000000000000000000000000000002", 2)).
			Add(digest.MustNewDigest("c", "00000000000000000000000000000003", 3)).
			Build()
		baseBlobAccess.EXPECT().FindMissing(ctx, digests).
			Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, digests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("SuccessiveFailure", func(t *testing.T) {
		digests := digest.NewSetBuilder().
			Add(digest.MustNewDigest("a", "00000000000000000000000000000001", 1)).
			Add(digest.MustNewDigest("b", "00000000000000000000000000000002", 2)).
			Add(digest.MustNewDigest("c", "00000000000000000000000000000003", 3)).
			Build()
		gomock.InOrder(
			baseBlobAccess.EXPECT().FindMissing(ctx, digests).Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("b", "00000000000000000000000000000002", 2)).
					Build(),
				nil),
			baseBlobAccess.EXPECT().FindMissing(
				ctx,
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("", "00000000000000000000000000000002", 2)).
					Build(),
			).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire")))

		_, err := blobAccess.FindMissing(ctx, digests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Call FindMissing() with a set of digests of various
		// instance names. Because the longest instance name
		// consists of two components, we should see up to three
		// calls of FindMissing() against the backend.
		//
		// We don't want this backend to yield a single large
		// FindMissing() call, because that has the downside of
		// requesting far more objects than necessary and will
		// also cause objects to be touched, regardless of
		// whether they are going to be accessed.
		digests := digest.NewSetBuilder().
			// Empty instance name.
			Add(digest.MustNewDigest("", "00000000000000000000000000000001", 1)).
			Add(digest.MustNewDigest("", "00000000000000000000000000000002", 2)).
			// Single component instance name.
			Add(digest.MustNewDigest("a", "10000000000000000000000000000001", 3)).
			Add(digest.MustNewDigest("a", "20000000000000000000000000000001", 4)).
			Add(digest.MustNewDigest("b", "10000000000000000000000000000001", 3)).
			Add(digest.MustNewDigest("b", "20000000000000000000000000000001", 4)).
			// Double component instance name.
			Add(digest.MustNewDigest("x/y", "30000000000000000000000000000001", 3)).
			Add(digest.MustNewDigest("x/y", "40000000000000000000000000000001", 4)).
			Add(digest.MustNewDigest("x/z", "30000000000000000000000000000001", 3)).
			Add(digest.MustNewDigest("x/z", "40000000000000000000000000000001", 4)).
			Build()
		gomock.InOrder(
			baseBlobAccess.EXPECT().FindMissing(ctx, digests).Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("", "00000000000000000000000000000001", 1)).
					Add(digest.MustNewDigest("a", "10000000000000000000000000000001", 3)).
					Add(digest.MustNewDigest("b", "10000000000000000000000000000001", 3)).
					Add(digest.MustNewDigest("x/y", "30000000000000000000000000000001", 3)).
					Add(digest.MustNewDigest("x/z", "30000000000000000000000000000001", 3)).
					Build(),
				nil),
			baseBlobAccess.EXPECT().FindMissing(
				ctx,
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("", "10000000000000000000000000000001", 3)).
					Add(digest.MustNewDigest("x", "30000000000000000000000000000001", 3)).
					Build()).Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("x", "30000000000000000000000000000001", 3)).
					Build(),
				nil),
			baseBlobAccess.EXPECT().FindMissing(
				ctx,
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("", "30000000000000000000000000000001", 3)).
					Build()).Return(
				digest.NewSetBuilder().
					Add(digest.MustNewDigest("", "30000000000000000000000000000001", 3)).
					Build(),
				nil))

		missing, err := blobAccess.FindMissing(ctx, digests)
		require.NoError(t, err)
		require.Equal(
			t,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("", "00000000000000000000000000000001", 1)).
				Add(digest.MustNewDigest("x/y", "30000000000000000000000000000001", 3)).
				Add(digest.MustNewDigest("x/z", "30000000000000000000000000000001", 3)).
				Build(),
			missing)
	})
}
