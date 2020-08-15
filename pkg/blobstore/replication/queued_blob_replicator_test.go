package replication_test

import (
	"context"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueuedBlobReplicatorReplicateSingle(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess(ctrl)
	baseReplicator := mock.NewMockBlobReplicator(ctrl)
	clock := mock.NewMockClock(ctrl)
	replicator := replication.NewQueuedBlobReplicator(
		source,
		baseReplicator,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet()))
	helloDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloDigests := helloDigest.ToSingletonSet()

	t.Run("Success", func(t *testing.T) {
		// The first time the object is requested, it should be
		// replicated in the background.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(3)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(nil)

		b := replicator.ReplicateSingle(ctx, helloDigest)
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)

		// The fact that the object has been replicated should
		// be cached. We should only perform the load during the
		// second attempt.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		clock.EXPECT().Now().Return(time.Unix(1060, 0))

		b = replicator.ReplicateSingle(ctx, helloDigest)
		data, err = b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)

		// A third request after the cache entry has expired
		// should trigger a replication once again.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		clock.EXPECT().Now().Return(time.Unix(1060, 1)).Times(3)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(nil)

		b = replicator.ReplicateSingle(ctx, helloDigest)
		data, err = b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SourceError", func(t *testing.T) {
		// Simulate the case where replication succeeds, but
		// serving the object back to the caller fails.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		clock.EXPECT().Now().Return(time.Unix(1200, 0)).Times(3)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(nil)

		b := replicator.ReplicateSingle(ctx, helloDigest)
		_, err := b.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)

		// Because replication did succeed, a second call should
		// not trigger another replication.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		clock.EXPECT().Now().Return(time.Unix(1260, 0))

		b = replicator.ReplicateSingle(ctx, helloDigest)
		_, err = b.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("ReplicationError", func(t *testing.T) {
		// Replication errors should be communicated back to the
		// caller, so that it may retry replicating.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		clock.EXPECT().Now().Return(time.Unix(1400, 0)).Times(2)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(status.Error(codes.Internal, "Server on fire"))

		b := replicator.ReplicateSingle(ctx, helloDigest)
		_, err := b.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Replication failed: Server on fire"), err)

		// Replication failures should not be cached. Another
		// replication should be triggered.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		clock.EXPECT().Now().Return(time.Unix(1401, 0)).Times(2)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(status.Error(codes.Internal, "Server on fire"))

		b = replicator.ReplicateSingle(ctx, helloDigest)
		_, err = b.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Replication failed: Server on fire"), err)
	})
}

func TestQueuedBlobReplicatorReplicateMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess(ctrl)
	baseReplicator := mock.NewMockBlobReplicator(ctrl)
	clock := mock.NewMockClock(ctrl)
	replicator := replication.NewQueuedBlobReplicator(
		source,
		baseReplicator,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet()))
	helloDigests := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet()

	t.Run("Success", func(t *testing.T) {
		// The object should be replicated when requested initially.
		clock.EXPECT().Now().Return(time.Unix(1000, 0)).Times(3)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(nil)
		require.NoError(t, replicator.ReplicateMultiple(ctx, helloDigests))

		// Once cached, replication requests should be ignored.
		clock.EXPECT().Now().Return(time.Unix(1060, 0))
		require.NoError(t, replicator.ReplicateMultiple(ctx, helloDigests))

		// Once expired, replication should be performed once more.
		clock.EXPECT().Now().Return(time.Unix(1060, 1)).Times(3)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(nil)
		require.NoError(t, replicator.ReplicateMultiple(ctx, helloDigests))
	})

	t.Run("Error", func(t *testing.T) {
		// Replication errors should not cause objects to be cached.
		clock.EXPECT().Now().Return(time.Unix(1200, 0)).Times(2)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(status.Error(codes.Internal, "Server on fire"))
		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			replicator.ReplicateMultiple(ctx, helloDigests))

		clock.EXPECT().Now().Return(time.Unix(1201, 0)).Times(2)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(status.Error(codes.Internal, "Server on fire"))
		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			replicator.ReplicateMultiple(ctx, helloDigests))
	})
}
