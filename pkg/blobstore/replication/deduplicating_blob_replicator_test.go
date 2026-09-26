package replication_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestDeduplicatingBlobReplicatorSingleDigest(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	base := mock.NewMockBlobReplicator(ctrl)
	sink := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	replicator := replication.NewDeduplicatingBlobReplicator(base, sink, digest.KeyWithoutInstance)

	helloDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigestSet := helloDigest.ToSingletonSet()

	t.Run("SuccessNoop", func(t *testing.T) {
		// If the sink reports the blob as present, no actual
		// replication should take place.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil)

		err := replicator.ReplicateMultiple(ctx, helloDigestSet)
		require.NoError(t, err)
	})

	t.Run("SuccessReplication", func(t *testing.T) {
		// If the sink reports the blob as absent, we should see
		// a replication take place.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, helloDigestSet).Return(nil)

		err := replicator.ReplicateMultiple(ctx, helloDigestSet)
		require.NoError(t, err)
	})

	t.Run("FindMissingError", func(t *testing.T) {
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, status.Error(codes.Internal, "Disk I/O failure"))

		err := replicator.ReplicateMultiple(ctx, helloDigestSet)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to check for the existence of blob 3-8b1a9953c4611296a827abf8c47804d7-5-hello prior to replicating: Disk I/O failure"), err)
	})

	t.Run("ReplicateMultipleError", func(t *testing.T) {
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, helloDigestSet).Return(status.Error(codes.Internal, "Disk I/O failure"))

		err := replicator.ReplicateMultiple(ctx, helloDigestSet)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to replicate blob 3-8b1a9953c4611296a827abf8c47804d7-5-hello: Disk I/O failure"), err)
	})

	t.Run("ParallelFailure", func(t *testing.T) {
		// In case we send requests in parallel for the same
		// blob, DeduplicatingBlobReplicator may attempt to
		// deduplicate requests.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, status.Error(codes.Internal, "Disk I/O failure")).Times(10)

		done := make(chan struct{}, 10)
		for i := 0; i < 10; i++ {
			go func() {
				err := replicator.ReplicateMultiple(ctx, helloDigestSet)
				testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to check for the existence of blob 3-8b1a9953c4611296a827abf8c47804d7-5-hello prior to replicating: Disk I/O failure"), err)

				done <- struct{}{}
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})

	t.Run("ParallelSuccess", func(t *testing.T) {
		// In case parallel replication requests for the same
		// blob succeed, other callers are permitted to skip
		// replication.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, helloDigestSet).Return(nil)
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil).MaxTimes(9)

		done := make(chan struct{}, 10)
		for i := 0; i < 10; i++ {
			go func() {
				err := replicator.ReplicateMultiple(ctx, helloDigestSet)
				require.NoError(t, err)

				done <- struct{}{}
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestDeduplicatingBlobReplicatorMultipleDigests(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	base := mock.NewMockBlobReplicator(ctrl)
	sink := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	replicator := replication.NewDeduplicatingBlobReplicator(base, sink, digest.KeyWithoutInstance)

	helloDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigestSet := helloDigest.ToSingletonSet()
	worldDigest := digest.MustNewDigest("world", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)
	worldDigestSet := worldDigest.ToSingletonSet()
	allDigests := digest.NewSetBuilder(0).Add(helloDigest).Add(worldDigest).Build()

	t.Run("MultipleDigestsSuccess", func(t *testing.T) {
		// Request the replication of two blobs. Because one of
		// the blobs is already present, we should only see
		// ReplicateMultiple() against the base replicator for
		// one of the two blobs.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil)
		sink.EXPECT().FindMissing(ctx, worldDigestSet).Return(worldDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, worldDigestSet).Return(nil)

		require.NoError(t, replicator.ReplicateMultiple(ctx, allDigests))
	})
}
