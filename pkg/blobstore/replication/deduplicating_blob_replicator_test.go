package replication_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeduplicatingBlobReplicatorReplicateSingle(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	base := mock.NewMockBlobReplicator(ctrl)
	sink := mock.NewMockBlobAccess(ctrl)
	replicator := replication.NewDeduplicatingBlobReplicator(base, sink, digest.KeyWithoutInstance)

	helloDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigestSet := helloDigest.ToSingletonSet()

	t.Run("SuccessNoop", func(t *testing.T) {
		// If the sink reports the blob as present, no actual
		// replication should take place.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil)
		sink.EXPECT().Get(ctx, helloDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SuccessReplication", func(t *testing.T) {
		// If the sink reports the blob as absent, we should see
		// a replication take place before reading the blob from
		// the sink.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, helloDigestSet)
		sink.EXPECT().Get(ctx, helloDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("FindMissingError", func(t *testing.T) {
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, status.Error(codes.Internal, "Disk I/O failure"))

		_, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to check for the existence of blob 8b1a9953c4611296a827abf8c47804d7-5-hello prior to replicating: Disk I/O failure"), err)
	})

	t.Run("ReplicateMultipleError", func(t *testing.T) {
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, helloDigestSet).Return(status.Error(codes.Internal, "Disk I/O failure"))

		_, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to replicate blob 8b1a9953c4611296a827abf8c47804d7-5-hello: Disk I/O failure"), err)
	})

	t.Run("GetError", func(t *testing.T) {
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil)
		sink.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Disk I/O failure")))

		_, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk I/O failure"), err)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		// If the sink reports that the object is present, but a
		// successive Get() call fails, the results from the
		// sink are inconsistent. This should not cause
		// NOT_FOUND errors to be returned to clients.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil)
		sink.EXPECT().Get(ctx, helloDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Key not found in bucket")))

		_, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Blob absent from sink after replication: Key not found in bucket"), err)
	})

	t.Run("ParallelFailure", func(t *testing.T) {
		// In case we send requests in parallel for the same
		// blob, DeduplicatingBlobReplicator may attempt to
		// deduplicate requests. This should, however, not be
		// performed in case these requests fail.
		//
		// If one caller fails to replicate a blob, it doesn't
		// necessarily mean that another callers are also unable
		// to do so. They may use different credentials,
		// timeouts, etc.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, status.Error(codes.Internal, "Disk I/O failure")).Times(10)

		done := make(chan struct{}, 10)
		for i := 0; i < 10; i++ {
			go func() {
				_, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
				testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to check for the existence of blob 8b1a9953c4611296a827abf8c47804d7-5-hello prior to replicating: Disk I/O failure"), err)

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
		//
		// TODO: Because we can't strongly influence the timing
		// of goroutines inside DeduplicatingBlobReplicator, we
		// may see up to nine unnecessary FindMissing() calls.
		// It would have been nice if this test could be written
		// in a deterministic fashion.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(helloDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, helloDigestSet)
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil).MaxTimes(9)
		sink.EXPECT().Get(ctx, helloDigest).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))).Times(10)

		done := make(chan struct{}, 10)
		for i := 0; i < 10; i++ {
			go func() {
				data, err := replicator.ReplicateSingle(ctx, helloDigest).ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)

				done <- struct{}{}
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}

func TestDeduplicatingBlobReplicatorReplicateComposite(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	base := mock.NewMockBlobReplicator(ctrl)
	sink := mock.NewMockBlobAccess(ctrl)
	replicator := replication.NewDeduplicatingBlobReplicator(base, sink, digest.KeyWithoutInstance)

	parentDigest := digest.MustNewDigest("hello", "3e25960a79dbc69b674cd4ec67a72c62", 11)
	parentDigestSet := parentDigest.ToSingletonSet()
	childDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)
	slicer := mock.NewMockBlobSlicer(ctrl)

	// Only a single test for the success case is provided, as the
	// tests for ReplicateSingle() provide enough coverage.

	t.Run("SuccessReplication", func(t *testing.T) {
		// If the sink reports the parent as absent, we should
		// see a replication take place before reading the child
		// blob from the sink.
		sink.EXPECT().FindMissing(ctx, parentDigestSet).Return(parentDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, parentDigestSet)
		sink.EXPECT().GetFromComposite(ctx, parentDigest, childDigest, slicer).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := replicator.ReplicateComposite(ctx, parentDigest, childDigest, slicer).ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})
}

func TestDeduplicatingBlobReplicatorReplicateMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	base := mock.NewMockBlobReplicator(ctrl)
	sink := mock.NewMockBlobAccess(ctrl)
	replicator := replication.NewDeduplicatingBlobReplicator(base, sink, digest.KeyWithoutInstance)

	helloDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigestSet := helloDigest.ToSingletonSet()
	worldDigest := digest.MustNewDigest("world", "f5a7924e621e84c9280a9a27e1bcb7f6", 5)
	worldDigestSet := worldDigest.ToSingletonSet()
	allDigests := digest.NewSetBuilder().Add(helloDigest).Add(worldDigest).Build()

	// Only a single test for the success case is provided, as the
	// tests for ReplicateSingle() provide enough coverage.

	t.Run("Success", func(t *testing.T) {
		// Request the replication of two blobs. Because one of
		// the blobs is already present, we should only see
		// ReplicateMultiple() against the base replicator for
		// one of the two blobs.
		sink.EXPECT().FindMissing(ctx, helloDigestSet).Return(digest.EmptySet, nil)
		sink.EXPECT().FindMissing(ctx, worldDigestSet).Return(worldDigestSet, nil)
		base.EXPECT().ReplicateMultiple(ctx, worldDigestSet)

		require.NoError(t, replicator.ReplicateMultiple(ctx, allDigests))
	})
}
