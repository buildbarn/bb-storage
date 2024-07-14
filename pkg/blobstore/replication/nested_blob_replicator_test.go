package replication_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/replication"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestNestedBlobReplicator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	replicator := mock.NewMockBlobReplicator(ctrl)
	nestedReplicator := replication.NewNestedBlobReplicator(replicator, digest.KeyWithoutInstance, 10000)

	t.Run("Nothing", func(t *testing.T) {
		// Replication returns immediately if nothing is enqueued.
		require.NoError(t, nestedReplicator.Replicate(ctx))
	})

	t.Run("Example", func(t *testing.T) {
		// Enqueue some objects that can be replicated.
		nestedReplicator.EnqueueAction(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3cd3b79f60145bdb838c8fda08b0f6a4", 1))
		nestedReplicator.EnqueueDirectory(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "006a8fcea3babf8b029e14faba3553f4", 2))
		nestedReplicator.EnqueueDirectory(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "73586ba4d59d7503bda905048f2ac409", 3))
		nestedReplicator.EnqueueTree(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7c44eaf20479782e179eb32f9aac16d9", 4))

		// Replicating data should all of the objects above, but
		// also their transitive dependencies to be replicated.
		replicator.EXPECT().ReplicateMultiple(ctx, digest.EmptySet).AnyTimes()
		replicator.EXPECT().ReplicateSingle(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3cd3b79f60145bdb838c8fda08b0f6a4", 1)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Action{
				CommandDigest: &remoteexecution.Digest{
					Hash:      "8b90d8d36617845efae5d045918eed4a",
					SizeBytes: 5,
				},
				InputRootDigest: &remoteexecution.Digest{
					Hash:      "e69b1393b62aacda2d46737aaffda809",
					SizeBytes: 6,
				},
			}, buffer.UserProvided))
		replicator.EXPECT().ReplicateSingle(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "006a8fcea3babf8b029e14faba3553f4", 2)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Directory{
				Files: []*remoteexecution.FileNode{
					{
						Name: "file1",
						Digest: &remoteexecution.Digest{
							Hash:      "6f881c3ef7c841fa5fe3f9e35fd8a745",
							SizeBytes: 7,
						},
					},
					{
						Name: "file2",
						Digest: &remoteexecution.Digest{
							Hash:      "211aa29e2a010eae1bb65b3eed479d6c",
							SizeBytes: 8,
						},
					},
					{
						Name: "file3",
						Digest: &remoteexecution.Digest{
							Hash:      "6f881c3ef7c841fa5fe3f9e35fd8a745",
							SizeBytes: 7,
						},
					},
				},
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name: "directory1",
						Digest: &remoteexecution.Digest{
							Hash:      "73586ba4d59d7503bda905048f2ac409",
							SizeBytes: 3,
						},
					},
					{
						Name: "directory2",
						Digest: &remoteexecution.Digest{
							Hash:      "878a1677dd14e1485c9c578e8251b9b8",
							SizeBytes: 9,
						},
					},
				},
				Symlinks: []*remoteexecution.SymlinkNode{
					{
						Name:   "symlink1",
						Target: "/etc/passwd",
					},
				},
			}, buffer.UserProvided))
		replicator.EXPECT().ReplicateSingle(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "73586ba4d59d7503bda905048f2ac409", 3)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Directory{}, buffer.UserProvided))
		replicator.EXPECT().ReplicateSingle(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7c44eaf20479782e179eb32f9aac16d9", 4)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Tree{
				Root: &remoteexecution.Directory{
					Files: []*remoteexecution.FileNode{
						{
							Name: "file4",
							Digest: &remoteexecution.Digest{
								Hash:      "66b39b7d2658407275b6a55ef403f3d0",
								SizeBytes: 10,
							},
						},
					},
					Directories: []*remoteexecution.DirectoryNode{
						{
							Name: "directory3",
							Digest: &remoteexecution.Digest{
								Hash:      "467bb234dc18788b2ceb6ef24ade2d94",
								SizeBytes: 11,
							},
						},
					},
				},
				Children: []*remoteexecution.Directory{
					{
						Files: []*remoteexecution.FileNode{
							{
								Name: "file4",
								Digest: &remoteexecution.Digest{
									Hash:      "13d059e4e8609ea76009df43ff5157d6",
									SizeBytes: 12,
								},
							},
						},
					},
				},
			}, buffer.UserProvided))
		replicator.EXPECT().ReplicateMultiple(
			ctx,
			digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b90d8d36617845efae5d045918eed4a", 5).ToSingletonSet())
		replicator.EXPECT().ReplicateSingle(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "e69b1393b62aacda2d46737aaffda809", 6)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Directory{}, buffer.UserProvided))
		replicator.EXPECT().ReplicateMultiple(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "6f881c3ef7c841fa5fe3f9e35fd8a745", 7)).
				Add(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "211aa29e2a010eae1bb65b3eed479d6c", 8)).
				Build())
		replicator.EXPECT().ReplicateSingle(ctx, digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "878a1677dd14e1485c9c578e8251b9b8", 9)).
			Return(buffer.NewProtoBufferFromProto(&remoteexecution.Directory{}, buffer.UserProvided))
		replicator.EXPECT().ReplicateMultiple(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "66b39b7d2658407275b6a55ef403f3d0", 10)).
				Add(digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "13d059e4e8609ea76009df43ff5157d6", 12)).
				Build())

		require.NoError(t, nestedReplicator.Replicate(ctx))
	})
}
