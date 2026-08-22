package cas_test

import (
	"context"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"go.uber.org/mock/gomock"
)

func TestNestedBlobReplicator(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	replicator := mock.NewMockReplicator(ctrl)
	actionReader := mock.NewMockMessageReader[*remoteexecution.Action](ctrl)
	directoryReader := mock.NewMockMessageReader[*remoteexecution.Directory](ctrl)
	treeReader := mock.NewMockStreamReader(ctrl)
	nestedReplicator := cas.NewNestedBlobReplicator(
		replicator,
		10000,
		actionReader,
		directoryReader,
		treeReader,
		digest.KeyWithoutInstance,
	)

	t.Run("Nothing", func(t *testing.T) {
		// Replication returns immediately if nothing is enqueued.
		require.NoError(t, nestedReplicator.Replicate(ctx))
	})

	t.Run("Example", func(t *testing.T) {
		actionDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "3cd3b79f60145bdb838c8fda08b0f6a4", 1)
		fullDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "006a8fcea3babf8b029e14faba3553f4", 2)
		emptyDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "73586ba4d59d7503bda905048f2ac409", 3)
		treeDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "7c44eaf20479782e179eb32f9aac16d9", 4)
		commandDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b90d8d36617845efae5d045918eed4a", 5)
		inputRootDirectoryDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "e69b1393b62aacda2d46737aaffda809", 6)
		file1Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "6f881c3ef7c841fa5fe3f9e35fd8a745", 7)
		file2Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "211aa29e2a010eae1bb65b3eed479d6c", 8)
		directory2Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "878a1677dd14e1485c9c578e8251b9b8", 9)
		file4Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "66b39b7d2658407275b6a55ef403f3d0", 10)
		file5Digest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "13d059e4e8609ea76009df43ff5157d6", 12)
		action := &remoteexecution.Action{
			CommandDigest: &remoteexecution.Digest{
				Hash:      "8b90d8d36617845efae5d045918eed4a",
				SizeBytes: 5,
			},
			InputRootDigest: &remoteexecution.Digest{
				Hash:      "e69b1393b62aacda2d46737aaffda809",
				SizeBytes: 6,
			},
		}
		fullDirectory := &remoteexecution.Directory{
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
		}
		emptyDir := &remoteexecution.Directory{}
		tree := &remoteexecution.Tree{
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
		}

		// Enqueue some objects that can be replicated.
		nestedReplicator.EnqueueAction(actionDigest)
		nestedReplicator.EnqueueDirectory(fullDirectoryDigest)
		nestedReplicator.EnqueueDirectory(emptyDirectoryDigest)
		nestedReplicator.EnqueueTree(treeDigest)

		// Replicating data should replicate all of the objects above,
		// but also cause their transitive dependencies to be
		// replicated.
		replicator.EXPECT().Replicate(ctx, digest.EmptySet).AnyTimes()

		replicator.EXPECT().Replicate(ctx, actionDigest.ToSingletonSet()).Return(nil)
		actionReader.EXPECT().ReadMessage(ctx, actionDigest).Return(action, nil)
		replicator.EXPECT().Replicate(ctx, commandDigest.ToSingletonSet()).Return(nil)

		replicator.EXPECT().Replicate(ctx, fullDirectoryDigest.ToSingletonSet()).Return(nil)
		directoryReader.EXPECT().ReadMessage(ctx, fullDirectoryDigest).Return(fullDirectory, nil)
		replicator.EXPECT().Replicate(ctx, digest.NewSetBuilder(2).Add(file1Digest).Add(file2Digest).Build()).Return(nil)

		replicator.EXPECT().Replicate(ctx, emptyDirectoryDigest.ToSingletonSet()).Return(nil)
		directoryReader.EXPECT().ReadMessage(ctx, emptyDirectoryDigest).Return(emptyDir, nil)

		replicator.EXPECT().Replicate(ctx, treeDigest.ToSingletonSet()).Return(nil)
		treeReader.EXPECT().ReadStream(ctx, treeDigest).Return(buffer.NewProtoBufferFromProto(tree, buffer.UserProvided).ToReader(), nil)
		replicator.EXPECT().Replicate(ctx, digest.NewSetBuilder(2).Add(file4Digest).Add(file5Digest).Build()).Return(nil)

		replicator.EXPECT().Replicate(ctx, inputRootDirectoryDigest.ToSingletonSet()).Return(nil)
		directoryReader.EXPECT().ReadMessage(ctx, inputRootDirectoryDigest).Return(emptyDir, nil)

		replicator.EXPECT().Replicate(ctx, directory2Digest.ToSingletonSet()).Return(nil)
		directoryReader.EXPECT().ReadMessage(ctx, directory2Digest).Return(emptyDir, nil)

		require.NoError(t, nestedReplicator.Replicate(ctx))
	})
}
