package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Empty", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(false).Return(componentWalker, nil)

		require.NoError(t, path.Resolve("", scopeWalker))
	})

	t.Run("Dot", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(false).Return(componentWalker, nil)

		require.NoError(t, path.Resolve(".", scopeWalker))
	})

	t.Run("SingleFileRelative", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(false).Return(componentWalker, nil)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("hello"))

		require.NoError(t, path.Resolve("hello", scopeWalker))
	})

	t.Run("SingleFileAbsolute", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(true).Return(componentWalker, nil)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("hello"))

		require.NoError(t, path.Resolve("/hello", scopeWalker))
	})

	t.Run("SingleDirectoryWithSlash", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(false).Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("hello")).
			Return(path.GotDirectory{Child: componentWalker2}, nil)

		require.NoError(t, path.Resolve("hello/", scopeWalker))
	})

	t.Run("SingleDirectoryWithSlashDot", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(false).Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("hello")).
			Return(path.GotDirectory{Child: componentWalker2}, nil)

		require.NoError(t, path.Resolve("hello/.", scopeWalker))
	})

	t.Run("MultipleComponents", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnScope(false).Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("a")).
			Return(path.GotDirectory{Child: componentWalker2}, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnUp().Return(componentWalker3, nil)
		componentWalker4 := mock.NewMockComponentWalker(ctrl)
		componentWalker3.EXPECT().OnDirectory(path.MustNewComponent("b")).
			Return(path.GotDirectory{Child: componentWalker4}, nil)
		componentWalker5 := mock.NewMockComponentWalker(ctrl)
		componentWalker4.EXPECT().OnDirectory(path.MustNewComponent("c")).
			Return(path.GotDirectory{Child: componentWalker5}, nil)
		componentWalker6 := mock.NewMockComponentWalker(ctrl)
		componentWalker5.EXPECT().OnUp().Return(componentWalker6, nil)
		componentWalker6.EXPECT().OnTerminal(path.MustNewComponent("d"))

		require.NoError(t, path.Resolve("./a////../b/c/../d", scopeWalker))
	})

	t.Run("SymlinkWithoutSlash", func(t *testing.T) {
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnScope(false).Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnTerminal(path.MustNewComponent("a")).
			Return(&path.GotSymlink{Parent: scopeWalker2, Target: "b"}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnScope(false).Return(componentWalker2, nil)
		componentWalker2.EXPECT().OnTerminal(path.MustNewComponent("b"))

		require.NoError(t, path.Resolve("a", scopeWalker1))
	})

	t.Run("SymlinkWithSlashInSymlink", func(t *testing.T) {
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnScope(false).Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnTerminal(path.MustNewComponent("a")).
			Return(&path.GotSymlink{Parent: scopeWalker2, Target: "b/"}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnScope(false).Return(componentWalker2, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnDirectory(path.MustNewComponent("b")).
			Return(path.GotDirectory{Child: componentWalker3}, nil)

		require.NoError(t, path.Resolve("a", scopeWalker1))
	})

	t.Run("SymlinkWithSlashInPath", func(t *testing.T) {
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnScope(false).Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("a")).
			Return(path.GotSymlink{Parent: scopeWalker2, Target: "b"}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnScope(false).Return(componentWalker2, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnDirectory(path.MustNewComponent("b")).
			Return(path.GotDirectory{Child: componentWalker3}, nil)

		require.NoError(t, path.Resolve("a/", scopeWalker1))
	})

	t.Run("SymlinkInSymlinkInSymlink", func(t *testing.T) {
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnScope(false).Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnTerminal(path.MustNewComponent("a")).
			Return(&path.GotSymlink{Parent: scopeWalker2, Target: "b/z"}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnScope(false).Return(componentWalker2, nil)
		scopeWalker3 := mock.NewMockScopeWalker(ctrl)
		componentWalker2.EXPECT().OnDirectory(path.MustNewComponent("b")).
			Return(path.GotSymlink{Parent: scopeWalker3, Target: "c/y"}, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		scopeWalker3.EXPECT().OnScope(false).Return(componentWalker3, nil)
		scopeWalker4 := mock.NewMockScopeWalker(ctrl)
		componentWalker3.EXPECT().OnDirectory(path.MustNewComponent("c")).
			Return(path.GotSymlink{Parent: scopeWalker4, Target: "x"}, nil)
		componentWalker4 := mock.NewMockComponentWalker(ctrl)
		scopeWalker4.EXPECT().OnScope(false).Return(componentWalker4, nil)
		componentWalker5 := mock.NewMockComponentWalker(ctrl)
		componentWalker4.EXPECT().OnDirectory(path.MustNewComponent("x")).
			Return(path.GotDirectory{Child: componentWalker5}, nil)
		componentWalker6 := mock.NewMockComponentWalker(ctrl)
		componentWalker5.EXPECT().OnDirectory(path.MustNewComponent("y")).
			Return(path.GotDirectory{Child: componentWalker6}, nil)
		componentWalker6.EXPECT().OnTerminal(path.MustNewComponent("z"))

		require.NoError(t, path.Resolve("a", scopeWalker1))
	})
}
