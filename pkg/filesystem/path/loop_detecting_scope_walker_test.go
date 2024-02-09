package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoopDetectingScopeWalker(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Loop", func(t *testing.T) {
		// A self-referential symbolic link should only cause a
		// finite number of expansions before failing.
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnRelative().Return(componentWalker, nil).Times(41)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("foo")).
			Return(&path.GotSymlink{Parent: scopeWalker, Target: "foo"}, nil).
			Times(41)

		require.Equal(
			t,
			status.Error(codes.InvalidArgument, "Maximum number of symbolic link redirections reached"),
			path.Resolve(path.MustNewUNIXParser("foo"), path.NewLoopDetectingScopeWalker(scopeWalker)))
	})

	t.Run("Success", func(t *testing.T) {
		// Simple case where a symbolic link is not self-referential.
		scopeWalker1 := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker1.EXPECT().OnAbsolute().Return(componentWalker1, nil)
		scopeWalker2 := mock.NewMockScopeWalker(ctrl)
		componentWalker1.EXPECT().OnTerminal(path.MustNewComponent("tmp")).
			Return(&path.GotSymlink{Parent: scopeWalker2, Target: "private/tmp"}, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		scopeWalker2.EXPECT().OnRelative().Return(componentWalker2, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnDirectory(path.MustNewComponent("private")).
			Return(path.GotDirectory{Child: componentWalker3, IsReversible: true}, nil)
		componentWalker3.EXPECT().OnTerminal(path.MustNewComponent("tmp")).
			Return(nil, nil)

		require.NoError(
			t,
			path.Resolve(path.MustNewUNIXParser("/tmp"), path.NewLoopDetectingScopeWalker(scopeWalker1)))
	})
}
