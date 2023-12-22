package completenesschecking_test

import (
	"context"
	"io"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/completenesschecking"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestCompletenessCheckingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	actionCache := mock.NewMockBlobAccess(ctrl)
	contentAddressableStorage := mock.NewMockBlobAccess(ctrl)
	completenessCheckingBlobAccess := completenesschecking.NewCompletenessCheckingBlobAccess(
		actionCache,
		contentAddressableStorage,
		/* batchSize = */ 5,
		/* maximumMessageSizeBytes = */ 1000,
		/* maximumTotalTreeSizeBytes = */ 10000)

	actionDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 123)

	t.Run("ActionCacheFailure", func(t *testing.T) {
		// Errors on the backing action cache should be passed
		// on directly.
		actionCache.EXPECT().Get(ctx, actionDigest).Return(buffer.NewBufferFromError(status.Error(codes.NotFound, "Action not found")))

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Action not found"), err)
	})

	t.Run("BadDigest", func(t *testing.T) {
		// In case the ActionResult or one of the referenced
		// Tree objects contains a malformed digest, act as if
		// the ActionResult did not exist. This should cause the
		// client to rebuild the action.
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&remoteexecution.ActionResult{
					StdoutDigest: &remoteexecution.Digest{
						Hash:      "this is a malformed hash",
						SizeBytes: 12,
					},
				},
				buffer.BackendProvided(dataIntegrityCallback.Call)))

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Action result contained malformed digest: Hash has length 24, while 32 characters were expected"), err)
	})

	t.Run("MissingInput", func(t *testing.T) {
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&remoteexecution.ActionResult{
					OutputFiles: []*remoteexecution.OutputFile{
						{
							Path: "bazel-out/foo.o",
							Digest: &remoteexecution.Digest{
								Hash:      "8b1a9953c4611296a827abf8c47804d7",
								SizeBytes: 5,
							},
						},
					},
					StderrDigest: &remoteexecution.Digest{
						Hash:      "6fc422233a40a75a1f028e11c3cd1140",
						SizeBytes: 7,
					},
				},
				buffer.BackendProvided(dataIntegrityCallback.Call)))
		contentAddressableStorage.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7)).
				Build(),
		).Return(
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5).ToSingletonSet(),
			nil)

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object 3-8b1a9953c4611296a827abf8c47804d7-5-hello referenced by the action result is not present in the Content Addressable Storage"), err)
	})

	t.Run("FindMissingError", func(t *testing.T) {
		// FindMissing() errors should get propagated.
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&remoteexecution.ActionResult{
					StderrDigest: &remoteexecution.Digest{
						Hash:      "6fc422233a40a75a1f028e11c3cd1140",
						SizeBytes: 7,
					},
				},
				buffer.BackendProvided(dataIntegrityCallback.Call)))
		contentAddressableStorage.EXPECT().FindMissing(
			ctx,
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6fc422233a40a75a1f028e11c3cd1140", 7).ToSingletonSet(),
		).Return(digest.EmptySet, status.Error(codes.Internal, "Hard disk has a case of the Mondays"))

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Failed to determine existence of child objects: Hard disk has a case of the Mondays"), err)
	})

	t.Run("GetTreeError", func(t *testing.T) {
		// GetTree() errors should get propagated.
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&remoteexecution.ActionResult{
					OutputDirectories: []*remoteexecution.OutputDirectory{
						{
							Path: "bazel-out/foo",
							TreeDigest: &remoteexecution.Digest{
								Hash:      "8b1a9953c4611296a827abf8c47804d7",
								SizeBytes: 5,
							},
						},
					},
				},
				buffer.BackendProvided(dataIntegrityCallback.Call)))
		contentAddressableStorage.EXPECT().Get(
			ctx,
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		).Return(buffer.NewBufferFromError(status.Error(codes.Internal, "Hard disk has a case of the Mondays")))

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Output directory \"bazel-out/foo\": Hard disk has a case of the Mondays"), err)
	})

	t.Run("GetTreeTooLarge", func(t *testing.T) {
		// ActionResult entries that reference Tree objects that
		// are too big to load should be treated as being
		// invalid, and thus invisible to the caller.
		dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&remoteexecution.ActionResult{
					OutputDirectories: []*remoteexecution.OutputDirectory{
						{
							Path: "bazel-out/foo",
							TreeDigest: &remoteexecution.Digest{
								Hash:      "7ef23d85401d061552b188ae0a87d7f8",
								SizeBytes: 1024 * 1024 * 1024,
							},
						},
					},
				},
				buffer.BackendProvided(dataIntegrityCallback.Call)))

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Combined size of all output directories exceeds maximum limit of 10000 bytes"), err)
	})

	t.Run("GetTreeDataCorruption", func(t *testing.T) {
		// Because Tree objects are processed in a streaming
		// fashion, it may be the case that we call
		// FindMissing() against the CAS, even though we later
		// discover that the Tree object was corrupted.
		//
		// This means that even if FindMissing() reports objects
		// as being absent, we cannot terminate immediately. We
		// must process the Tree object in its entirety.
		dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback1.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&remoteexecution.ActionResult{
					OutputDirectories: []*remoteexecution.OutputDirectory{
						{
							Path: "bazel-out/foo",
							TreeDigest: &remoteexecution.Digest{
								Hash:      "8f0450aa5f4602d93968daba6f2e7611",
								SizeBytes: 4000,
							},
						},
					},
				},
				buffer.BackendProvided(dataIntegrityCallback1.Call)))

		treeReader := mock.NewMockReadCloser(ctrl)
		treeReader.EXPECT().Read(gomock.Any()).
			DoAndReturn(func(p []byte) (int, error) {
				treeData, err := proto.Marshal(&remoteexecution.Tree{
					Root: &remoteexecution.Directory{
						Files: []*remoteexecution.FileNode{
							{
								Digest: &remoteexecution.Digest{
									Hash:      "024ced29f1fdef2f644f34a071ade5be",
									SizeBytes: 1,
								},
							},
							{
								Digest: &remoteexecution.Digest{
									Hash:      "8b3b146b1c4df062a2dc35168cbf4ce6",
									SizeBytes: 2,
								},
							},
							{
								Digest: &remoteexecution.Digest{
									Hash:      "4a4a6ebb3f8b062653cb957cbdc047d9",
									SizeBytes: 3,
								},
							},
							{
								Digest: &remoteexecution.Digest{
									Hash:      "69778ed3e4dcf4e0c40df49e4ca5bd37",
									SizeBytes: 4,
								},
							},
							{
								Digest: &remoteexecution.Digest{
									Hash:      "ff7816e0353299e801a30e37aee1758c",
									SizeBytes: 5,
								},
							},
						},
					},
				})
				require.NoError(t, err)
				return copy(p, treeData), nil
			})
		treeReader.EXPECT().Read(gomock.Any()).
			DoAndReturn(func(p []byte) (int, error) {
				return copy(p, "Garbage"), io.EOF
			})
		treeReader.EXPECT().Close()
		dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback2.EXPECT().Call(false)
		treeDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8f0450aa5f4602d93968daba6f2e7611", 4000)
		contentAddressableStorage.EXPECT().Get(ctx, treeDigest).Return(
			buffer.NewCASBufferFromReader(treeDigest, treeReader, buffer.BackendProvided(dataIntegrityCallback2.Call)))
		contentAddressableStorage.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(treeDigest).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "024ced29f1fdef2f644f34a071ade5be", 1)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b3b146b1c4df062a2dc35168cbf4ce6", 2)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4a4a6ebb3f8b062653cb957cbdc047d9", 3)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "69778ed3e4dcf4e0c40df49e4ca5bd37", 4)).
				Build(),
		).Return(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "4a4a6ebb3f8b062653cb957cbdc047d9", 3).ToSingletonSet(), nil)

		_, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Output directory \"bazel-out/foo\": Buffer is 210 bytes in size, while 4000 bytes were expected"), err)
	})

	t.Run("Success", func(t *testing.T) {
		// Successful checking of existence of dependencies.
		// Below is an ActionResult that contains five
		// references to blobs, of which one is a Tree object.
		// The Tree contains two references to files. As the
		// batch size of FindMissing() is five, we should see
		// two FindMissing() calls (as ceil((5 + 2) / 5) == 2).
		actionResult := remoteexecution.ActionResult{
			OutputFiles: []*remoteexecution.OutputFile{
				{
					Path: "bazel-out/foo.o",
					Digest: &remoteexecution.Digest{
						Hash:      "38837949e2518a6e8a912ffb29942788",
						SizeBytes: 10,
					},
				},
				{
					Path: "bazel-out/foo.d",
					Digest: &remoteexecution.Digest{
						Hash:      "ebbbb099e9d2f7892d97ab3640ae8283",
						SizeBytes: 9,
					},
				},
			},
			OutputDirectories: []*remoteexecution.OutputDirectory{
				{
					Path: "bazel-out/foo",
					TreeDigest: &remoteexecution.Digest{
						Hash:      "8b1a9953c4611296a827abf8c47804d7",
						SizeBytes: 200,
					},
					RootDirectoryDigest: &remoteexecution.Digest{
						Hash:      "f7b00e64e49a13e36a5d50ec2e1ab21d",
						SizeBytes: 130,
					},
				},
			},
			StdoutDigest: &remoteexecution.Digest{
				Hash:      "136de6de72514772b9302d4776e5c3d2",
				SizeBytes: 4,
			},
			StderrDigest: &remoteexecution.Digest{
				Hash:      "41d7247285b686496aa91b56b4c48395",
				SizeBytes: 11,
			},
		}
		dataIntegrityCallback1 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback1.EXPECT().Call(true)
		actionCache.EXPECT().Get(ctx, actionDigest).Return(
			buffer.NewProtoBufferFromProto(
				&actionResult,
				buffer.BackendProvided(dataIntegrityCallback1.Call)))
		dataIntegrityCallback2 := mock.NewMockDataIntegrityCallback(ctrl)
		dataIntegrityCallback2.EXPECT().Call(true)
		contentAddressableStorage.EXPECT().Get(
			ctx,
			digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 200),
		).Return(buffer.NewProtoBufferFromProto(&remoteexecution.Tree{
			Root: &remoteexecution.Directory{
				// Directory digests should not be part of
				// FindMissing(), as references to directories
				// are contained within the Tree object itself.
				Directories: []*remoteexecution.DirectoryNode{
					{
						Name: "sub1",
						Digest: &remoteexecution.Digest{
							Hash:      "bdcfcea2c9e3d753463abd000dab2495",
							SizeBytes: 40,
						},
					},
					{
						Name: "sub2",
						Digest: &remoteexecution.Digest{
							Hash:      "d41d8cd98f00b204e9800998ecf8427e",
							SizeBytes: 0,
						},
					},
				},
				Files: []*remoteexecution.FileNode{
					{
						Digest: &remoteexecution.Digest{
							Hash:      "eda14e187a768b38eda999457c9cca1e",
							SizeBytes: 6,
						},
					},
				},
			},
			Children: []*remoteexecution.Directory{
				{
					Files: []*remoteexecution.FileNode{
						{
							Digest: &remoteexecution.Digest{
								Hash:      "6c396013ff0ebff6a2a96cdc20a4ba4c",
								SizeBytes: 5,
							},
						},
					},
				},
				{},
			},
		}, buffer.BackendProvided(dataIntegrityCallback2.Call)))
		contentAddressableStorage.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "38837949e2518a6e8a912ffb29942788", 10)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "ebbbb099e9d2f7892d97ab3640ae8283", 9)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 200)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "f7b00e64e49a13e36a5d50ec2e1ab21d", 130)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "136de6de72514772b9302d4776e5c3d2", 4)).
				Build(),
		).Return(digest.EmptySet, nil)
		contentAddressableStorage.EXPECT().FindMissing(
			ctx,
			digest.NewSetBuilder().
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "41d7247285b686496aa91b56b4c48395", 11)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "eda14e187a768b38eda999457c9cca1e", 6)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "bdcfcea2c9e3d753463abd000dab2495", 40)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "6c396013ff0ebff6a2a96cdc20a4ba4c", 5)).
				Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "d41d8cd98f00b204e9800998ecf8427e", 0)).
				Build(),
		).Return(digest.EmptySet, nil)

		actualResult, err := completenessCheckingBlobAccess.Get(ctx, actionDigest).ToProto(&remoteexecution.ActionResult{}, 1000)
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &actionResult, actualResult)
	})
}
