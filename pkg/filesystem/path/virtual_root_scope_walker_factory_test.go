package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestVirtualRootScopeWalkerFactoryCreationFailure(t *testing.T) {
	t.Run("InvalidRootPath", func(t *testing.T) {
		_, err := path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("foo"),
			map[string]string{},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve root path: Path is relative, while an absolute path was expected"), err)
	})

	t.Run("InvalidAliasPath", func(t *testing.T) {
		_, err := path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("/foo"),
			map[string]string{"bar": "baz"},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias path \"bar\": Path is relative, while an absolute path was expected"), err)

		_, err = path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("/foo"),
			map[string]string{"/foo": "baz"},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias path \"/foo\": Path resides at or below an already registered path"), err)

		_, err = path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("/foo"),
			map[string]string{"/foo/bar": "baz"},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias path \"/foo/bar\": Path resides at or below an already registered path"), err)

		_, err = path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("/foo/bar"),
			map[string]string{"/foo": "baz"},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias path \"/foo\": Path resides above an already registered path"), err)

		_, err = path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("/foo"),
			map[string]string{"/bar/..": "."},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias path \"/bar/..\": Last component is not a valid filename"), err)
	})

	t.Run("InvalidAliasTarget", func(t *testing.T) {
		_, err := path.NewVirtualRootScopeWalkerFactory(
			path.UNIXFormat.NewParser("/foo"),
			map[string]string{"/bar": "/qux"},
		)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Failed to resolve alias target \"/qux\": Path is absolute, while a relative path was expected"), err)
	})
}

func TestVirtualRootScopeWalkerFactoryCreationSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)

	factory, err := path.NewVirtualRootScopeWalkerFactory(
		path.UNIXFormat.NewParser("/root"),
		map[string]string{"/alias": "target"},
	)
	require.NoError(t, err)

	t.Run("Relative", func(t *testing.T) {
		// Relative paths should be completely unaffected by the
		// virtual root directory.
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnRelative().Return(componentWalker, nil)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("hello"))

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("hello"), factory.New(scopeWalker)))
	})

	t.Run("Absolute", func(t *testing.T) {
		// Absolute paths should have their prefix stripped.
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnAbsolute().Return(componentWalker, nil)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("hello"))

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("/root/hello"), factory.New(scopeWalker)))
	})

	t.Run("AbsoluteViaAlias", func(t *testing.T) {
		// Absolute paths that go via an alias should also end
		// up in the virtual root directory.
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnAbsolute().Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("target")).
			Return(path.GotDirectory{
				Child:        componentWalker2,
				IsReversible: true,
			}, nil)
		componentWalker2.EXPECT().OnTerminal(path.MustNewComponent("hello"))

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("/alias/hello"), factory.New(scopeWalker)))
	})

	t.Run("Outside", func(t *testing.T) {
		// Absolute paths pointing to a location outside the
		// root directory should not generate any calls against
		// the ScopeWalker.
		scopeWalker := mock.NewMockScopeWalker(ctrl)

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("/"), factory.New(scopeWalker)))
		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("/hello"), factory.New(scopeWalker)))
	})

	t.Run("SymlinkRelative", func(t *testing.T) {
		// Symbolic links containing relative paths should also be
		// completely unaffected by the virtual root directory.
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnRelative().Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnTerminal(path.MustNewComponent("a")).
			Return(&path.GotSymlink{
				Parent: scopeWalker2,
				Target: path.UNIXFormat.NewParser("b"),
			}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnRelative().Return(componentWalker2, nil)
		componentWalker2.EXPECT().OnTerminal(path.MustNewComponent("b"))

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("a"), factory.New(scopeWalker1)))
	})

	t.Run("SymlinkAbsolute", func(t *testing.T) {
		// Symbolic links containing absolute paths should also
		// have their prefix stripped.
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnRelative().Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnTerminal(path.MustNewComponent("a")).
			Return(&path.GotSymlink{
				Parent: scopeWalker2,
				Target: path.UNIXFormat.NewParser("/root/b"),
			}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnAbsolute().Return(componentWalker2, nil)
		componentWalker2.EXPECT().OnTerminal(path.MustNewComponent("b"))

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("a"), factory.New(scopeWalker1)))
	})

	t.Run("SymlinkAbsolute", func(t *testing.T) {
		// Symbolic links containing absolute paths pointing to
		// a location outside the root directory should not
		// generate any calls against the successive ScopeWalker.
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnRelative().Return(componentWalker, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("a")).
			Return(&path.GotSymlink{
				Parent: scopeWalker2,
				Target: path.UNIXFormat.NewParser("/hello"),
			}, nil)

		require.NoError(t, path.Resolve(path.UNIXFormat.NewParser("a"), factory.New(scopeWalker1)))
	})
}
