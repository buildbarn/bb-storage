package mirrored_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/mirrored"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMirroredBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	replicatorAToB := mock.NewMockBlobReplicator(ctrl)
	replicatorBToA := mock.NewMockBlobReplicator(ctrl)
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)

	t.Run("Success", func(t *testing.T) {
		// Requests should alternate between backends to spread
		// the load between backends equally.
		gomock.InOrder(
			backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))),
			backendB.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))),
			backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))),
		)

		blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)
		for i := 0; i < 3; i++ {
			data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello world"), data)
		}
	})

	t.Run("NotFoundBoth", func(t *testing.T) {
		// Simulate the case where a blob is not present in both
		// backends. It will try to synchronize the blob from
		// backend B to backend A, but this will fail.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		replicatorBToA.EXPECT().ReplicateSingle(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))

		blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), err)
	})

	t.Run("RepairSuccess", func(t *testing.T) {
		// The blob is only present in the second backend. It
		// will get synchronized into the first.
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		replicatorBToA.EXPECT().ReplicateSingle(ctx, blobDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello world")))

		blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)
		data, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello world"), data)
	})

	t.Run("ErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))

		// In case of fatal errors, the name of the backend
		// should be prepended.
		blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend A: Server on fire"), err)
	})

	t.Run("ErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().Get(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Blob not found")))
		replicatorBToA.EXPECT().ReplicateSingle(ctx, blobDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))

		blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)
		_, err := blobAccess.Get(ctx, blobDigest).ToByteSlice(100)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend B: Server on fire"), err)
	})
}

func TestMirroredBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	replicatorAToB := mock.NewMockBlobReplicator(ctrl)
	replicatorBToA := mock.NewMockBlobReplicator(ctrl)
	blobDigest := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)

	t.Run("Success", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(100)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello world"), data)
				return nil
			})

		require.NoError(t, blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("ErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return nil
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "Backend A: Server on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})

	t.Run("ErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return nil
			})
		backendB.EXPECT().Put(gomock.Any(), blobDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		require.Equal(
			t,
			status.Error(codes.Internal, "Backend B: Server on fire"),
			blobAccess.Put(ctx, blobDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))
	})
}

func TestMirroredBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	backendA := mock.NewMockBlobAccess(ctrl)
	backendB := mock.NewMockBlobAccess(ctrl)
	replicatorAToB := mock.NewMockBlobReplicator(ctrl)
	replicatorBToA := mock.NewMockBlobReplicator(ctrl)
	digestNone := digest.MustNewDigest("default", "64ec88ca00b268e5ba1a35678a1b5316d212f4f366b2477232534a8aeca37f3c", 11)
	digestA := digest.MustNewDigest("default", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	digestB := digest.MustNewDigest("default", "522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2", 9)
	digestBoth := digest.MustNewDigest("default", "9c6079651d4062b6811f93061cb6a768a60e51d714bddffee99b1173c6580580", 5)
	allDigests := digest.NewSetBuilder().Add(digestNone).Add(digestA).Add(digestB).Add(digestBoth).Build()
	onlyOnA := digestA.ToSingletonSet()
	onlyOnB := digestB.ToSingletonSet()
	missingFromA := digest.NewSetBuilder().Add(digestNone).Add(digestB).Build()
	missingFromB := digest.NewSetBuilder().Add(digestNone).Add(digestA).Build()
	blobAccess := mirrored.NewMirroredBlobAccess(backendA, backendB, replicatorAToB, replicatorBToA)

	t.Run("Success", func(t *testing.T) {
		// Listings of both backends should be requested.
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		// Blobs missing in one backend, but present in the
		// other should be exchanged.
		replicatorAToB.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnA).Return(nil)
		replicatorBToA.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnB).Return(nil)

		// The intersection of missing blobs in the backends
		// should be returned.
		missing, err := blobAccess.FindMissing(ctx, allDigests)
		require.NoError(t, err)
		require.Equal(t, digestNone.ToSingletonSet(), missing)
	})

	t.Run("FindMissingErrorBackendA", func(t *testing.T) {
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, status.Error(codes.Internal, "Backend A: Server on fire"), err)
	})

	t.Run("FindMissingErrorBackendB", func(t *testing.T) {
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(digest.EmptySet, status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		require.Equal(t, status.Error(codes.Internal, "Backend B: Server on fire"), err)
	})

	t.Run("ReplicateErrorAToB", func(t *testing.T) {
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		replicatorAToB.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnA).Return(status.Error(codes.Internal, "Server on fire"))
		replicatorBToA.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnB).Return(nil)

		_, err := blobAccess.FindMissing(ctx, allDigests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to synchronize from backend A to backend B: Server on fire"), err)
	})

	t.Run("ReplicateErrorBToA", func(t *testing.T) {
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		replicatorAToB.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnA).Return(nil)
		replicatorBToA.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnB).Return(status.Error(codes.Internal, "Server on fire"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to synchronize from backend B to backend A: Server on fire"), err)
	})

	t.Run("InconsistentBackendA", func(t *testing.T) {
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		replicatorAToB.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnA).Return(status.Error(codes.NotFound, "Object e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 not found"))
		replicatorBToA.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnB)

		_, err := blobAccess.FindMissing(ctx, allDigests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend A returned inconsistent results while synchronizing: Object e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 not found"), err)
	})

	t.Run("InconsistentBackendB", func(t *testing.T) {
		backendA.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromA, nil)
		backendB.EXPECT().FindMissing(gomock.Any(), allDigests).Return(missingFromB, nil)

		replicatorAToB.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnA)
		replicatorBToA.EXPECT().ReplicateMultiple(gomock.Any(), onlyOnB).Return(status.Error(codes.NotFound, "Object 522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2 not found"))

		_, err := blobAccess.FindMissing(ctx, allDigests)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Backend B returned inconsistent results while synchronizing: Object 522b44d647b6989f60302ef755c277e508d5bcc38f05e139906ebdb03a5b19f2 not found"), err)
	})
}
