package replication_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLocalBlobReplicatorReplicateSingle(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess(ctrl)
	sink := mock.NewMockBlobAccess(ctrl)
	replicator := replication.NewLocalBlobReplicator(source, sink)
	helloDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		// Data should be read from the source and written into
		// the sink.
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})

		// Data should also be returned to the caller.
		b := replicator.ReplicateSingle(ctx, helloDigest)
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("SourceError", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				_, err := b.ToByteSlice(10)
				require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
				return status.Error(codes.Internal, "Failed to read input: Server on fire")
			})

		// The error from the source should be returned to the caller.
		b := replicator.ReplicateSingle(ctx, helloDigest)
		_, err := b.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
	})

	t.Run("SinkError", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Internal, "Server on fire")
			})

		// The error from the sink could in theory be ignored,
		// but doing so would make misbehaviour of the system
		// less evident. The error message should be prefixed to
		// be able to disambiguate from source errors.
		b := replicator.ReplicateSingle(ctx, helloDigest)
		_, err := b.ToByteSlice(10)
		require.Equal(t, status.Error(codes.Internal, "Replication failed: Server on fire"), err)
	})
}

func TestLocalBlobReplicatorReplicateMultiple(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	source := mock.NewMockBlobAccess(ctrl)
	sink := mock.NewMockBlobAccess(ctrl)
	replicator := replication.NewLocalBlobReplicator(source, sink)
	helloDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)
	worldDigest := digest.MustNewDigest("world", "f5a7924e621e84c9280a9a27e1bcb7f6", 5)

	t.Run("Success", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})

		source.EXPECT().Get(ctx, worldDigest).Return(
			buffer.NewValidatedBufferFromByteSlice([]byte("World")))
		sink.EXPECT().Put(ctx, worldDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(10)
				require.NoError(t, err)
				require.Equal(t, []byte("World"), data)
				return nil
			})

		require.NoError(
			t,
			replicator.ReplicateMultiple(
				ctx,
				digest.NewSetBuilder().
					Add(helloDigest).
					Add(worldDigest).
					Build()))
	})

	t.Run("ReplicationError", func(t *testing.T) {
		source.EXPECT().Get(ctx, helloDigest).Return(
			buffer.NewBufferFromError(status.Error(codes.Internal, "Server on fire")))
		sink.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				_, err := b.ToByteSlice(10)
				require.Equal(t, status.Error(codes.Internal, "Server on fire"), err)
				return status.Error(codes.Internal, "Failed to read input: Server on fire")
			})

		// Error messages should be prefixed with the digest of
		// the object that was being replicated.
		require.Equal(
			t,
			status.Error(codes.Internal, "8b1a9953c4611296a827abf8c47804d7-5-hello: Failed to read input: Server on fire"),
			replicator.ReplicateMultiple(ctx, helloDigest.ToSingletonSet()))
	})
}
