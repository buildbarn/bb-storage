package sharding_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestShardingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	shard0 := mock.NewMockBlobAccess(ctrl)
	shard1 := mock.NewMockBlobAccess(ctrl)
	shardSelector := mock.NewMockShardSelector(ctrl)
	blobAccess := sharding.NewShardingBlobAccess(
		map[string]blobstore.BlobAccess{
			"shard0": shard0,
			"shard1": shard1,
		},
		shardSelector,
		/* hashInitialization = */ 0x62994904405896a1)

	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	llDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "5b54c0a045f179bcbbbc9abcb8b5cd4c", 2)

	t.Run("GetFailure", func(t *testing.T) {
		// Errors should be prefixed with the shard key
		shardSelector.EXPECT().GetShard(uint64(0x7118d6877ee9ee3d)).Return("shard1")
		shard1.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewBufferFromError(status.Error(codes.Unavailable, "Server offline")))

		_, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(1000)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Shard shard1: Server offline"), err)
	})

	t.Run("GetSuccess", func(t *testing.T) {
		shardSelector.EXPECT().GetShard(uint64(0x7118d6877ee9ee3d)).Return("shard0")
		shard0.EXPECT().Get(ctx, helloDigest).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

		data, err := blobAccess.Get(ctx, helloDigest).ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("GetFromCompositeSuccess", func(t *testing.T) {
		// For reads from composite objects, the sharding needs
		// to be based on the parent digest. That digest was
		// used to upload the object to storage.
		shardSelector.EXPECT().GetShard(uint64(0x7118d6877ee9ee3d)).Return("shard0")
		slicer := mock.NewMockBlobSlicer(ctrl)
		shard0.EXPECT().GetFromComposite(ctx, helloDigest, llDigest, slicer).
			Return(buffer.NewValidatedBufferFromByteSlice([]byte("ll")))

		data, err := blobAccess.GetFromComposite(ctx, helloDigest, llDigest, slicer).ToByteSlice(1000)
		require.NoError(t, err)
		require.Equal(t, []byte("ll"), data)
	})

	t.Run("PutFailure", func(t *testing.T) {
		// Errors should be prefixed with a shard key.
		shardSelector.EXPECT().GetShard(uint64(0x7118d6877ee9ee3d)).Return("shard1")
		shard1.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				b.Discard()
				return status.Error(codes.Unavailable, "Server offline")
			})

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Shard shard1: Server offline"),
			blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	t.Run("PutSuccess", func(t *testing.T) {
		shardSelector.EXPECT().GetShard(uint64(0x7118d6877ee9ee3d)).Return("shard0")
		shard0.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, b buffer.Buffer) error {
				data, err := b.ToByteSlice(1000)
				require.NoError(t, err)
				require.Equal(t, []byte("Hello"), data)
				return nil
			})

		require.NoError(t, blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	})

	digest1 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "21f843aefbfb88627ec2cad9e8f1f49a", 1)
	digest2 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "48f2503cf369373b0631da97fb9de1c1", 2)
	digest3 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "942a5b4164c26ae5d57a4f9526dcfca4", 3)
	digest4 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "f8f3da00ff2862082bddbb15300343f6", 4)

	t.Run("FindMissingFailure", func(t *testing.T) {
		// Digests provided to FindMissing() are partitioned,
		// causing up to one call per backend. If one of the
		// backends reports failure, we immediately cancel the
		// context for remaining requests, and return the first
		// error that occurred.
		shardSelector.EXPECT().GetShard(uint64(0xe4780eee2c3e5c4d)).Return("shard0")
		shardSelector.EXPECT().GetShard(uint64(0xb1e63d21c14e3f12)).Return("shard1")
		shard0.EXPECT().FindMissing(
			gomock.Any(),
			digest1.ToSingletonSet(),
		).Return(digest.EmptySet, status.Error(codes.Unavailable, "Server offline"))
		shard1.EXPECT().FindMissing(
			gomock.Any(),
			digest2.ToSingletonSet(),
		).DoAndReturn(func(ctx context.Context, digests digest.Set) (digest.Set, error) {
			<-ctx.Done()
			require.Equal(t, context.Canceled, ctx.Err())
			return digest.EmptySet, status.Error(codes.Canceled, "Context canceled")
		})

		_, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder().Add(digest1).Add(digest2).Build(),
		)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Shard shard0: Server offline"), err)
	})

	t.Run("FindMissingSuccess", func(t *testing.T) {
		shardSelector.EXPECT().GetShard(uint64(0xe4780eee2c3e5c4d)).Return("shard0")
		shardSelector.EXPECT().GetShard(uint64(0xb1e63d21c14e3f12)).Return("shard0")
		shardSelector.EXPECT().GetShard(uint64(0x71fb8268edc4f6e9)).Return("shard1")
		shardSelector.EXPECT().GetShard(uint64(0xc7a206e6fcdfda55)).Return("shard1")
		shard0.EXPECT().FindMissing(
			gomock.Any(),
			digest.NewSetBuilder().Add(digest1).Add(digest2).Build(),
		).Return(digest1.ToSingletonSet(), nil)
		shard1.EXPECT().FindMissing(
			gomock.Any(),
			digest.NewSetBuilder().Add(digest3).Add(digest4).Build(),
		).Return(digest3.ToSingletonSet(), nil)

		missing, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest1).Add(digest2).
				Add(digest3).Add(digest4).
				Build(),
		)
		require.NoError(t, err)
		require.Equal(t, digest.NewSetBuilder().Add(digest1).Add(digest3).Build(), missing)
	})
}
