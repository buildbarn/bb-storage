package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestAbsoluteScopeWalker(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("Absolute", func(t *testing.T) {
		componentWalker := mock.NewMockComponentWalker(ctrl)
		componentWalker.EXPECT().OnTerminal(path.MustNewComponent("hello"))

		require.NoError(t, path.Resolve(path.NewUNIXParser("/hello"), path.NewAbsoluteScopeWalker(componentWalker)))
	})

	t.Run("Relative", func(t *testing.T) {
		componentWalker := mock.NewMockComponentWalker(ctrl)

		require.Equal(
			t,
			status.Error(codes.InvalidArgument, "Path is relative, while an absolute path was expected"),
			path.Resolve(path.NewUNIXParser("hello"), path.NewAbsoluteScopeWalker(componentWalker)))
	})
}
