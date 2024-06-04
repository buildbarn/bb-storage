package path_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func mustGetWindowsString(p path.Stringer) string {
	s, err := p.GetWindowsString()
	if err != nil {
		panic(err)
	}
	return s
}

func TestBuilder(t *testing.T) {
	ctrl := gomock.NewController(t)

	// The following paths should remain completely identical when
	// resolved without making any assumptions about the layout of
	// the underlying file system. ".." elements should not be
	// removed from paths.
	t.Run("UNIXIdentity", func(t *testing.T) {
		for _, p := range []string{
			".",
			"..",
			"/",
			"hello",
			"hello/",
			"hello/..",
			"/hello/",
			"/hello/..",
			"/hello/../world",
			"/hello/../world/",
			"/hello/../world/foo",
		} {
			t.Run(p, func(t *testing.T) {
				builder1, scopewalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.MustNewUNIXParser(p), scopewalker1))
				require.Equal(t, p, builder1.GetUNIXString())

				builder2, scopewalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopewalker2))
				require.Equal(t, p, builder2.GetUNIXString())
			})
		}
	})

	t.Run("WindowsParseUNIXPaths", func(t *testing.T) {
		for _, data := range [][]string{
			{".", "."},
			{"..", ".."},
			{"/", "\\"},
			{"hello", "hello"},
			{"hello/", "hello\\"},
			{"hello/..", "hello\\.."},
			{"/hello/", "\\hello\\"},
			{"/hello/..", "\\hello\\.."},
			{"/hello/../world", "\\hello\\..\\world"},
			{"/hello/../world/", "\\hello\\..\\world\\"},
			{"/hello/../world/foo", "\\hello\\..\\world\\foo"},
		} {
			p := data[0]
			expected := data[1]
			t.Run(p, func(t *testing.T) {
				// Windows Parser, compare Windows and UNIX string identity.
				builder1, scopewalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.NewWindowsParser(p), scopewalker1))
				require.Equal(t, expected, mustGetWindowsString(builder1))
				require.Equal(t, p, builder1.GetUNIXString())

				builder2, scopewalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopewalker2))
				require.Equal(t, expected, mustGetWindowsString(builder2))
				require.Equal(t, p, builder2.GetUNIXString())
			})
		}
	})

	t.Run("WindowsIdentity", func(t *testing.T) {
		for _, p := range []string{
			"C:\\",
			"C:\\hello\\",
			"C:\\hello\\..",
			"C:\\hello\\..\\world",
			"C:\\hello\\..\\world\\",
			"C:\\hello\\..\\world\\foo",
			"C:\\hello\\..\\world\\foo",
		} {
			t.Run(p, func(t *testing.T) {
				builder1, scopewalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.NewWindowsParser(p), scopewalker1))
				require.Equal(t, p, mustGetWindowsString(builder1))

				builder2, scopewalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopewalker2))
				require.Equal(t, p, mustGetWindowsString(builder2))
			})
		}
	})

	t.Run("WindowsParseAndWriteUNIXPaths", func(t *testing.T) {
		for _, data := range [][]string{
			{"C:\\", "/"},
			{"C:\\.", "/"},
			{"C:\\hello\\", "/hello/"},
			{"C:\\hello\\.", "/hello/"},
			{"C:\\hello\\..", "/hello/.."},
			{"C:\\hello\\.\\world", "/hello/world"},
			{"C:\\hello\\..\\world", "/hello/../world"},
			{"C:\\hello\\..\\world\\", "/hello/../world/"},
			{"C:\\hello\\..\\world\\foo", "/hello/../world/foo"},
			{"C:\\hello\\\\..\\world\\foo", "/hello/../world/foo"},
		} {
			p := data[0]
			expected := data[1]
			t.Run(p, func(t *testing.T) {
				builder1, scopewalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.NewWindowsParser(p), scopewalker1))
				require.Equal(t, expected, builder1.GetUNIXString())

				builder2, scopewalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopewalker2))
				require.Equal(t, expected, builder2.GetUNIXString())
			})
		}
	})

	t.Run("WindowsParseCasing", func(t *testing.T) {
		for _, data := range [][]string{
			{"./bar", "bar"},
			{"./bar\\", "bar\\"},
			{"c:", "C:\\"},
			{"c:.", "C:\\"},
			{"c:Hello", "C:\\Hello"},
			{"c:\\", "C:\\"},
			{"c:\\.", "C:\\"},
			{"c:\\Hello\\", "C:\\Hello\\"},
			{"c:\\Hello\\.", "C:\\Hello\\"},
			{"c:\\Hello\\..", "C:\\Hello\\.."},
			{"c:\\Hello\\.\\world", "C:\\Hello\\world"},
			{"c:\\Hello\\..\\world", "C:\\Hello\\..\\world"},
			{"c:\\Hello\\..\\world", "C:\\Hello\\..\\world"},
			{"c:\\Hello\\..\\world\\", "C:\\Hello\\..\\world\\"},
			{"c:\\Hello\\..\\world\\foo", "C:\\Hello\\..\\world\\foo"},
			{"c:\\\\Hello\\\\..\\world\\foo", "C:\\Hello\\..\\world\\foo"},
		} {
			p := data[0]
			expected := data[1]
			t.Run(p, func(t *testing.T) {
				builder1, scopewalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.NewWindowsParser(p), scopewalker1))
				require.Equal(t, expected, mustGetWindowsString(builder1))

				builder2, scopewalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopewalker2))
				require.Equal(t, expected, mustGetWindowsString(builder2))
			})
		}
	})

	// The following paths can be normalized, even when making no
	// assumptions about the layout of the underlying file system.
	t.Run("UNIXNormalized", func(t *testing.T) {
		for from, to := range map[string]string{
			"":            ".",
			"./":          ".",
			"./.":         ".",
			"../":         "..",
			"../.":        "..",
			"//":          "/",
			"/.":          "/",
			"/./":         "/",
			"/..":         "/",
			"/../":        "/",
			"/hello/.":    "/hello/",
			"/hello/../.": "/hello/..",
		} {
			t.Run(from, func(t *testing.T) {
				builder1, scopeWalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.MustNewUNIXParser(from), scopeWalker1))
				require.Equal(t, to, builder1.GetUNIXString())

				builder2, scopeWalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopeWalker2))
				require.Equal(t, to, builder2.GetUNIXString())
			})
		}
	})

	t.Run("WindowsNormalized", func(t *testing.T) {
		for from, to := range map[string]string{
			"":            ".",
			"./":          ".",
			"./.":         ".",
			"../":         "..",
			"../.":        "..",
			"//":          "\\",
			"/.":          "\\",
			"/./":         "\\",
			"/..":         "\\",
			"/../":        "\\",
			"/hello/.":    "\\hello\\",
			"/hello/../.": "\\hello\\..",
		} {
			t.Run(from, func(t *testing.T) {
				builder1, scopeWalker1 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.MustNewUNIXParser(from), scopeWalker1))
				require.Equal(t, to, mustGetWindowsString(builder1))

				builder2, scopeWalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopeWalker2))
				require.Equal(t, to, mustGetWindowsString(builder2))
			})
		}
	})

	// Paths generated by joining with RootBuilder should start the
	// resolution process at the root directory.
	t.Run("Root", func(t *testing.T) {
		for from, to := range map[string]string{
			"":         "/",
			"hello":    "/hello",
			"/hello":   "/hello",
			"..":       "/",
			"../hello": "/hello",
		} {
			t.Run(from, func(t *testing.T) {
				builder1, scopeWalker1 := path.RootBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(path.MustNewUNIXParser(from), scopeWalker1))
				require.Equal(t, to, builder1.GetUNIXString())

				builder2, scopeWalker2 := path.EmptyBuilder.Join(path.VoidScopeWalker)
				require.NoError(t, path.Resolve(builder1, scopeWalker2))
				require.Equal(t, to, builder2.GetUNIXString())
			})
		}
	})

	// When OnDirectory() returns a GotDirectory response with
	// IsReversible == true, we're permitted to remove the component
	// entirely when successive OnUp() calls are performed. This
	// means that in the case of "hello/..", the resulting path may
	// be ".".
	t.Run("Reversible1", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnRelative().Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("hello")).
			Return(path.GotDirectory{Child: componentWalker2, IsReversible: true}, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnUp().Return(componentWalker3, nil)

		builder, s := path.EmptyBuilder.Join(scopeWalker)
		require.NoError(t, path.Resolve(path.MustNewUNIXParser("hello/.."), s))
		require.Equal(t, ".", builder.GetUNIXString())
	})

	// The same as before, "../hello/.." may evaluate to ".." in
	// case the "hello" directory is reversible.
	t.Run("Reversible2", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnRelative().Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnUp().Return(componentWalker2, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnDirectory(path.MustNewComponent("hello")).
			Return(path.GotDirectory{Child: componentWalker3, IsReversible: true}, nil)
		componentWalker4 := mock.NewMockComponentWalker(ctrl)
		componentWalker3.EXPECT().OnUp().Return(componentWalker4, nil)

		builder, s := path.EmptyBuilder.Join(scopeWalker)
		require.NoError(t, path.Resolve(path.MustNewUNIXParser("../hello/.."), s))
		require.Equal(t, "..", builder.GetUNIXString())
	})

	// In case "/hello/world/.." is evaluated and "world" is
	// reversible, the result may be simplified to "/hello/". We
	// should leave the trailing slash in place, as "/hello" may be
	// a symbolic link.
	t.Run("Reversible3", func(t *testing.T) {
		scopeWalker := mock.NewMockScopeWalker(ctrl)
		componentWalker1 := mock.NewMockComponentWalker(ctrl)
		scopeWalker.EXPECT().OnAbsolute().Return(componentWalker1, nil)
		componentWalker2 := mock.NewMockComponentWalker(ctrl)
		componentWalker1.EXPECT().OnDirectory(path.MustNewComponent("hello")).
			Return(path.GotDirectory{Child: componentWalker2, IsReversible: false}, nil)
		componentWalker3 := mock.NewMockComponentWalker(ctrl)
		componentWalker2.EXPECT().OnDirectory(path.MustNewComponent("world")).
			Return(path.GotDirectory{Child: componentWalker3, IsReversible: true}, nil)
		componentWalker4 := mock.NewMockComponentWalker(ctrl)
		componentWalker3.EXPECT().OnUp().Return(componentWalker4, nil)

		builder, s := path.EmptyBuilder.Join(scopeWalker)
		require.NoError(t, path.Resolve(path.MustNewUNIXParser("/hello/world/.."), s))
		require.Equal(t, "/hello/", builder.GetUNIXString())
	})
}
