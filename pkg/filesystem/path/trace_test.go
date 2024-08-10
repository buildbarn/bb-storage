package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestTrace(t *testing.T) {
	t.Run("UNIXIdentity", func(t *testing.T) {
		var p1 *path.Trace
		require.Equal(t, ".", p1.GetUNIXString())

		p2 := p1.Append(path.MustNewComponent("a"))
		require.Equal(t, "a", p2.GetUNIXString())

		p3 := p2.Append(path.MustNewComponent("b"))
		require.Equal(t, "a/b", p3.GetUNIXString())

		p4 := p3.Append(path.MustNewComponent("c"))
		require.Equal(t, "a/b/c", p4.GetUNIXString())
	})

	t.Run("WindowsUNIXLikeIdentity", func(t *testing.T) {
		var p1 *path.Trace
		require.Equal(t, ".", mustGetWindowsString(p1))

		p2 := p1.Append(path.MustNewComponent("a"))
		require.Equal(t, "a", mustGetWindowsString(p2))

		p3 := p2.Append(path.MustNewComponent("b"))
		require.Equal(t, "a\\b", mustGetWindowsString(p3))

		p4 := p3.Append(path.MustNewComponent("c"))
		require.Equal(t, "a\\b\\c", mustGetWindowsString(p4))
	})

	t.Run("WindowsPathValidation", func(t *testing.T) {
		{
			var p1 *path.Trace
			require.Equal(t, ".", mustGetWindowsString(p1))

			p2 := p1.Append(path.MustNewComponent("a:"))
			p3 := p2.Append(path.MustNewComponent("b"))

			_, err := p3.GetWindowsString()
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid pathname component \"a:\": Pathname component contains reserved characters"), err)
		}
		{
			var p1 *path.Trace
			require.Equal(t, ".", mustGetWindowsString(p1))
			p2 := p1.Append(path.MustNewComponent("b?"))
			_, err := p2.GetWindowsString()
			testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Invalid pathname component \"b?\": Pathname component contains reserved characters"), err)
		}
	})

	t.Run("ToList", func(t *testing.T) {
		var p1 *path.Trace
		require.Empty(t, p1.ToList())

		p2 := p1.Append(path.MustNewComponent("a"))
		require.Equal(t, []path.Component{
			path.MustNewComponent("a"),
		}, p2.ToList())

		p3 := p2.Append(path.MustNewComponent("b"))
		require.Equal(t, []path.Component{
			path.MustNewComponent("a"),
			path.MustNewComponent("b"),
		}, p3.ToList())

		p4 := p3.Append(path.MustNewComponent("c"))
		require.Equal(t, []path.Component{
			path.MustNewComponent("a"),
			path.MustNewComponent("b"),
			path.MustNewComponent("c"),
		}, p4.ToList())
	})
}
