package replication_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestLocalBlobReplicator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess[*buffer.Chunk](ctrl)
	sink := mock.NewMockBlobAccess[*buffer.Chunk](ctrl)
	replicator := replication.NewLocalBlobReplicator(source, sink)

	helloDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	worldDigest := digest.MustNewDigest("world", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)

	t.Run("Success", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(buffer.NewChunk(nil, []byte("Hello")), nil)
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *buffer.Chunk) error {
				data, err := c.GetBytes(ctx)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			},
		)

		source.EXPECT().Get(ctx, worldDigest).Return(buffer.NewChunk(nil, []byte("World")), nil)
		sink.EXPECT().Put(ctx, worldDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *buffer.Chunk) error {
				data, err := c.GetBytes(ctx)
				require.NoError(t, err)
				require.Equal(t, []byte("World"), data)
				return nil
			},
		)

		require.NoError(
			t,
			replicator.ReplicateMultiple(
				ctx,
				digest.NewSetBuilder(0).
					Add(helloDigest).
					Add(worldDigest).
					Build(),
			),
		)
	})

	t.Run("SourceError", func(t *testing.T) {
		// Because BlobAccess is no longer lazy, an error on Get()
		// immediately aborts the replication for that blob without calling Put().
		source.EXPECT().Get(ctx, helloDigest).Return(nil, status.Error(codes.Internal, "Server on fire"))

		// Error messages should be prefixed with the digest of
		// the object that was being replicated.
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "3-8b1a9953c4611296a827abf8c47804d7-5-hello: Server on fire"),
			replicator.ReplicateMultiple(ctx, helloDigest.ToSingletonSet()),
		)
	})

	t.Run("SinkError", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(buffer.NewChunk(nil, []byte("Hello")), nil)
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *buffer.Chunk) error {
				return status.Error(codes.Internal, "Disk full")
			},
		)

		// Error messages should be prefixed with the digest of
		// the object that was being replicated.
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "3-8b1a9953c4611296a827abf8c47804d7-5-hello: Disk full"),
			replicator.ReplicateMultiple(ctx, helloDigest.ToSingletonSet()),
		)
	})
}
