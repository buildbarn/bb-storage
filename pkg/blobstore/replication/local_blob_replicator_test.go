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

func TestLocalBlobReplicatorReplicateSingle(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	sink := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	replicator := replication.NewLocalBlobReplicator(source, sink)
	helloDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		// Data should be read from the source and written into
		// the sink.
		source.EXPECT().Get(ctx, helloDigest).Return(
			chunk.NewChunk(nil, []byte("Hello")), nil,
		)
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *chunk.Chunk) error {
				require.Equal(t, []byte("Hello"), c.GetBytes())
				return nil
			},
		)

		// Data should also be returned to the caller.
		c, err := replicator.ReplicateSingle(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), c.GetBytes())
	})

	t.Run("SourceError", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(
			nil, status.Error(codes.Internal, "Server on fire"),
		)

		// The error from the source should be returned to the caller.
		c, err := replicator.ReplicateSingle(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Server on fire"), err)
		require.Nil(t, c)
	})

	t.Run("SinkError", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(
			chunk.NewChunk(nil, []byte("Hello")), nil,
		)
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *chunk.Chunk) error {
				return status.Error(codes.Internal, "Server on fire")
			},
		)

		// The error from the sink could in theory be ignored,
		// but doing so would make misbehaviour of the system
		// less evident. The error message should be prefixed to
		// be able to disambiguate from source errors.
		c, err := replicator.ReplicateSingle(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "3-8b1a9953c4611296a827abf8c47804d7-5-hello: Server on fire"), err)
		require.Equal(t, []byte("Hello"), c.GetBytes())
	})
}

func TestLocalBlobReplicatorReplicateMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	sink := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	replicator := replication.NewLocalBlobReplicator(source, sink)
	helloDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	worldDigest := digest.MustNewDigest("world", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)

	t.Run("Success", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(chunk.NewChunk(nil, []byte("Hello")), nil)
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *chunk.Chunk) error {
				data := c.GetBytes()
				require.Equal(t, []byte("Hello"), data)
				return nil
			},
		)

		source.EXPECT().Get(ctx, worldDigest).Return(chunk.NewChunk(nil, []byte("World")), nil)
		sink.EXPECT().Put(ctx, worldDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *chunk.Chunk) error {
				data := c.GetBytes()
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
		source.EXPECT().Get(ctx, helloDigest).Return(chunk.NewChunk(nil, []byte("Hello")), nil)
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, c *chunk.Chunk) error {
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
