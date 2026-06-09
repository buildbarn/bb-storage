package replication_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestQueuedBlobReplicator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess[*buffer.Chunk](ctrl)
	baseReplicator := mock.NewMockBlobReplicator(ctrl)
	clock := mock.NewMockClock(ctrl)
	replicator := replication.NewQueuedBlobReplicator(
		source,
		baseReplicator,
		digest.NewExistenceCache(clock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet[string]()),
	)
	helloDigests := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet()

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
			replicator.ReplicateMultiple(ctx, helloDigests),
		)

		clock.EXPECT().Now().Return(time.Unix(1201, 0)).Times(2)
		baseReplicator.EXPECT().ReplicateMultiple(ctx, helloDigests).Return(status.Error(codes.Internal, "Server on fire"))
		require.Equal(
			t,
			status.Error(codes.Internal, "Server on fire"),
			replicator.ReplicateMultiple(ctx, helloDigests),
		)
	})
}
