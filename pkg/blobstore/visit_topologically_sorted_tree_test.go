package blobstore_test

import (
	"bytes"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestVisitTopologicallySortedTree(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("EmptyTree", func(t *testing.T) {
		// Tree objects must contain at least one directory.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)

		rootArgument := 123
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Tree does not contain any directories"),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer(nil),
				digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("MissingRootDirectory", func(t *testing.T) {
		// The first directory must always be the root directory.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)

		tree, err := proto.Marshal(&remoteexecution.Tree{
			Children: []*remoteexecution.Directory{{
				Files: []*remoteexecution.FileNode{{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "d8ef1593a6a7947c61baacd6945cbbcda0ca6b90cab290c4fdc628391f9c3a21",
						SizeBytes: 12345,
					},
				}},
			}},
		})
		require.NoError(t, err)
		rootArgument := 123
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 2 at offset 2 size 80: Expected field number 1"),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer(tree),
				digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("MalformedRootDirectory", func(t *testing.T) {
		// Individual entries in the tree must be valid REv2
		// Directory messages.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)

		rootArgument := 123
		testutil.RequirePrefixedStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 1 at offset 2 size 5: Failed to unmarshal message: "),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer([]byte{byte(blobstore.TreeRootFieldNumber<<3) | byte(protowire.BytesType), 5, 'H', 'e', 'l', 'l', 'o'}),
				digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("InvalidChildDigest", func(t *testing.T) {
		// Digests of child directories must be well-formed.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)

		tree, err := proto.Marshal(&remoteexecution.Tree{
			Root: &remoteexecution.Directory{
				Directories: []*remoteexecution.DirectoryNode{{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "fc20a67a98bfd79b463f00c03e0cd8f5",
						SizeBytes: 42,
					},
				}},
			},
			Children: []*remoteexecution.Directory{{
				Files: []*remoteexecution.FileNode{{
					Name: "hello",
					Digest: &remoteexecution.Digest{
						Hash:      "d8ef1593a6a7947c61baacd6945cbbcda0ca6b90cab290c4fdc628391f9c3a21",
						SizeBytes: 12345,
					},
				}},
			}},
		})
		require.NoError(t, err)
		rootArgument := 123
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 1 at offset 2 size 47: Invalid digest for child directory \"hello\": Hash has length 32, while 64 characters were expected"),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer(tree),
				digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("VisitorFailure", func(t *testing.T) {
		// Errors returned by the visitor must be propagated.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)
		rootDirectory := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{{
				Name: "foo",
				Digest: &remoteexecution.Digest{
					Hash:      "e732cb0c4b229c11f314a8f5d0091300e8863ad85b6120a30441424ec05ee570",
					SizeBytes: 42,
				},
			}},
		}
		rootArgument := 123
		visitor.EXPECT().Call(testutil.EqProto(t, rootDirectory), &rootArgument, gomock.Len(0)).
			Return(status.Error(codes.Internal, "Cannot download \"foo\""))

		tree, err := proto.Marshal(&remoteexecution.Tree{
			Root: rootDirectory,
		})
		require.NoError(t, err)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Internal, "Field with number 1 at offset 2 size 77: Cannot download \"foo\""),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer(tree),
				digest.MustNewFunction("", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("UnexpectedChildDirectory", func(t *testing.T) {
		// Trees can't contain child directories that aren't
		// referenced by any parent.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)
		rootDirectory := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{{
				Name: "foo",
				Digest: &remoteexecution.Digest{
					Hash:      "8459db5934c6c4c57e2370091f52cab4e7d2c5fd9189fad04e65bcf7da271632",
					SizeBytes: 1200,
				},
			}},
		}
		rootArgument := 123
		visitor.EXPECT().Call(testutil.EqProto(t, rootDirectory), &rootArgument, gomock.Len(0))

		tree, err := proto.Marshal(&remoteexecution.Tree{
			Root: rootDirectory,
			Children: []*remoteexecution.Directory{{
				Files: []*remoteexecution.FileNode{{
					Name: "bar",
					Digest: &remoteexecution.Digest{
						Hash:      "d78246e76afcbd45f2b9177363be8f0b69562258efd3ce10b01537b1fd29e88a",
						SizeBytes: 1300,
					},
				}},
			}},
		})
		require.NoError(t, err)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "Field with number 2 at offset 82 size 78: Directory has digest \"1-0dcd71a395913d28ea219014f2b3c290a5b6b3208f75d4652cc8189633ef1edd-78-hello\", which was not expected"),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer(tree),
				digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("MissingChildDirectory", func(t *testing.T) {
		// Directories can't refer to children that aren't
		// present in the tree.
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)
		rootDirectory := &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "a",
					Digest: &remoteexecution.Digest{
						Hash:      "6e229505e4229f925c2e0db995bdb423831db29244c6b52afe48eef4df8652c2",
						SizeBytes: 42,
					},
				},
				{
					Name: "b",
					Digest: &remoteexecution.Digest{
						Hash:      "8210f9ba247ca671f58d112980ddbc2d56bbe34071854bcf9f64b1523c9d9968",
						SizeBytes: 43,
					},
				},
			},
		}
		rootArgument := 123
		defaultArgument := 0
		visitor.EXPECT().Call(testutil.EqProto(t, rootDirectory), &rootArgument, []*int{&defaultArgument, &defaultArgument})

		tree, err := proto.Marshal(&remoteexecution.Tree{
			Root: rootDirectory,
		})
		require.NoError(t, err)
		testutil.RequireEqualStatus(
			t,
			status.Error(codes.InvalidArgument, "At least 2 more directories were expected"),
			blobstore.VisitTopologicallySortedTree(
				bytes.NewBuffer(tree),
				digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256),
				/* maximumDirectorySizeBytes = */ 10000,
				&rootArgument,
				visitor.Call))
	})

	t.Run("Success", func(t *testing.T) {
		visitor := mock.NewMockIntTreeDirectoryVisitor(ctrl)
		rootArgument := 123
		defaultArgument := 0
		directory1 := &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "a",
					Digest: &remoteexecution.Digest{
						Hash:      "a04ac895fa2f73839ffeb85ac1bccfdea48fe533edd53fc7ee29cd4d887cb808",
						SizeBytes: 225,
					},
				},
				{
					Name: "b",
					Digest: &remoteexecution.Digest{
						Hash:      "a04ac895fa2f73839ffeb85ac1bccfdea48fe533edd53fc7ee29cd4d887cb808",
						SizeBytes: 225,
					},
				},
			},
		}
		visitor.EXPECT().Call(testutil.EqProto(t, directory1), &rootArgument, []*int{&defaultArgument, &defaultArgument})

		directory2 := &remoteexecution.Directory{
			Directories: []*remoteexecution.DirectoryNode{
				{
					Name: "a",
					Digest: &remoteexecution.Digest{
						Hash:      "040d506676723f18bfa788eb192be1249b0915507fe1cc1bdd2a531e353689c2",
						SizeBytes: 79,
					},
				},
				{
					Name: "b",
					Digest: &remoteexecution.Digest{
						Hash:      "040d506676723f18bfa788eb192be1249b0915507fe1cc1bdd2a531e353689c2",
						SizeBytes: 79,
					},
				},
				{
					Name: "c",
					Digest: &remoteexecution.Digest{
						Hash:      "040d506676723f18bfa788eb192be1249b0915507fe1cc1bdd2a531e353689c2",
						SizeBytes: 79,
					},
				},
			},
		}
		visitor.EXPECT().Call(testutil.EqProto(t, directory2), &defaultArgument, []*int{&defaultArgument, &defaultArgument, &defaultArgument})

		directory3 := &remoteexecution.Directory{
			Files: []*remoteexecution.FileNode{{
				Name: "hello",
				Digest: &remoteexecution.Digest{
					Hash:      "173341c24df2cda18b84228f409a93ed5d20c1d37ab33dff39ee010027ae93fe",
					SizeBytes: 5,
				},
			}},
		}
		visitor.EXPECT().Call(testutil.EqProto(t, directory3), &defaultArgument, gomock.Len(0))

		tree, err := proto.Marshal(&remoteexecution.Tree{
			Root: directory1,
			Children: []*remoteexecution.Directory{
				directory2,
				directory3,
			},
		})
		require.NoError(t, err)
		require.NoError(t, blobstore.VisitTopologicallySortedTree(
			bytes.NewBuffer(tree),
			digest.MustNewFunction("hello", remoteexecution.DigestFunction_SHA256),
			/* maximumDirectorySizeBytes = */ 10000,
			&rootArgument,
			visitor.Call))
	})
}
