package readfallback_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/readfallback"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReadFallbackBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	primary := mock.NewMockBlobAccess(ctrl)
	secondary := mock.NewMockBlobAccess(ctrl)
	replicator := replication.NewNoopBlobReplicator(secondary)
	blobAccess := readfallback.NewReadFallbackBlobAccess(primary, secondary, replicator)
	helloDigest := digest.MustNewDigest("instance", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("PrimarySuccess", func(t *testing.T) {
		// The primary backend is able to serve the object.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		// The primary backend has a hard failure. This should
		// not cause it to access the secondary backend, as that
		// would introduce non-determinism.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Primary: I/O error"), err)
	})

	t.Run("SecondarySuccess", func(t *testing.T) {
		// The primary backend does not have the object. This
		// causes it to read it from the secondary backend.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))
		secondary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SecondaryFailure", func(t *testing.T) {
		// The primary backend does not have the data. This
		// causes it to read from the secondary backend, which
		// subsequently fails.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))
		secondary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Secondary: I/O error"), err)
	})

	t.Run("NotFound", func(t *testing.T) {
		// Both backends don't have the data. There is no need
		// to prefix the error message with 'Primary' or
		// 'Secondary' to disambiguate.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))
		secondary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.NotFound, "Object not found"), err)
	})
}

func TestReadFallbackBlobAccessReplication(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	primary := mock.NewMockBlobAccess(ctrl)
	secondary := mock.NewMockBlobAccess(ctrl)
	replicator := mock.NewMockBlobReplicator(ctrl)
	blobAccess := readfallback.NewReadFallbackBlobAccess(primary, secondary, replicator)
	helloDigest := digest.MustNewDigest("instance", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("PrimarySuccess", func(t *testing.T) {
		// The primary backend is able to serve the object.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		// The primary backend has a hard failure. This should
		// not cause it to access the secondary backend, as that
		// would introduce non-determinism.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Primary: I/O error"), err)
	})

	t.Run("SecondarySuccess", func(t *testing.T) {
		// The primary backend does not have the object. This
		// causes it to read it from the secondary backend.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))
		replicator.EXPECT().ReplicateSingle(ctx, helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SecondaryFailure", func(t *testing.T) {
		// The primary backend does not have the data. This
		// causes it to read from the secondary backend, which
		// subsequently fails.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))
		replicator.EXPECT().ReplicateSingle(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Internal, "I/O error")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.Internal, "Secondary: I/O error"), err)
	})

	t.Run("NotFound", func(t *testing.T) {
		// Both backends don't have the data. There is no need
		// to prefix the error message with 'Primary' or
		// 'Secondary' to disambiguate.
		primary.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))
		replicator.EXPECT().ReplicateSingle(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Object not found")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(100)
		require.Equal(t, status.Error(codes.NotFound, "Object not found"), err)
	})
}

func TestReadFallbackBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	primary := mock.NewMockBlobAccess(ctrl)
	secondary := mock.NewMockBlobAccess(ctrl)
	blobAccess := readfallback.NewReadFallbackBlobAccess(primary, secondary, nil)
	helloDigest := digest.MustNewDigest("instance", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		// Writes should always go to the primary backend. The
		// secondary backend is effectively read-only.
		primary.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
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

	t.Run("Failure", func(t *testing.T) {
		// There is no need to prefix anything to error messages
		// returned by the primary backend, as there is no
		// ambiguity which backend returned the error.
		primary.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "I/O error")
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "I/O error"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})
}

func TestReadFallbackBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	primary := mock.NewMockBlobAccess(ctrl)
	secondary := mock.NewMockBlobAccess(ctrl)
	blobAccess := readfallback.NewReadFallbackBlobAccess(primary, secondary, nil)

	allDigests := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", "00000000000000000000000000000000", 100)).
		Add(digest.MustNewDigest("instance", "00000000000000000000000000000001", 101)).
		Add(digest.MustNewDigest("instance", "00000000000000000000000000000002", 102)).
		Build()
	missingFromPrimary := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", "00000000000000000000000000000000", 100)).
		Add(digest.MustNewDigest("instance", "00000000000000000000000000000001", 101)).
		Build()
	missingFromBoth := digest.NewSetBuilder().
		Add(digest.MustNewDigest("instance", "00000000000000000000000000000000", 100)).
		Build()

	t.Run("Success", func(t *testing.T) {
		// Both backends should be queried. Only the missing
		// digests of the primary backend are passed on to the
		// secondary backend, so that load on the secondary
		// backend is reduced to a minimum.
		primary.EXPECT().FindMissing(ctx, allDigests).
			Return(missingFromPrimary, nil)
		secondary.EXPECT().FindMissing(ctx, missingFromPrimary).
			Return(missingFromBoth, nil)

		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, missingFromBoth, missing)
	})

	t.Run("PrimaryFailure", func(t *testing.T) {
		primary.EXPECT().FindMissing(ctx, allDigests).
			Return(digest.EmptySet, status.Error(codes.Internal, "I/O error"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, status.Error(codes.Internal, "Primary: I/O error"), err)
	})

	t.Run("SecondaryFailure", func(t *testing.T) {
		primary.EXPECT().FindMissing(ctx, allDigests).
			Return(missingFromPrimary, nil)
		secondary.EXPECT().FindMissing(ctx, missingFromPrimary).
			Return(digest.EmptySet, status.Error(codes.Internal, "I/O error"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, status.Error(codes.Internal, "Secondary: I/O error"), err)
	})
}
