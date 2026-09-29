//go:build darwin || freebsd || linux

package filesystem

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestLocalDirectoryRemoveChildrenMissingEntry(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	marker := filepath.Join(outside, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))
	for _, name := range []string{"disappearing", "surviving"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(root, name, "file"), nil, 0o600))
	}
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "outside")))
	directory, err := NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	d := directory.(*localDirectory)

	names, err := d.readdirnames()
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"disappearing", "surviving", "outside"}, names)
	// Process the missing entry first: swallowing ENOENT around the entire
	// traversal would leave the surviving siblings behind.
	for i, name := range names {
		if name == "disappearing" {
			names[0], names[i] = names[i], names[0]
			break
		}
	}
	require.NoError(t, os.RemoveAll(filepath.Join(root, "disappearing")))
	var stat syscall.Stat_t
	require.NoError(t, syscall.Fstat(d.fd, &stat))
	require.NoError(t, d.removeChildren(rawDeviceNumber(stat.Dev), names))

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

func TestLocalDirectoryRemoveAllMissing(t *testing.T) {
	d, err := NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	require.NoError(t, d.RemoveAll(path.MustNewComponent("missing")))
}

func TestLocalDirectoryRemovalClosed(t *testing.T) {
	directory, err := NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
	require.NoError(t, err)
	d := directory.(*localDirectory)
	require.NoError(t, d.Close())
	require.ErrorIs(t, d.removeChildren(0, []string{"missing"}), syscall.EBADF)
	require.ErrorIs(t, d.RemoveAllChildren(), syscall.EBADF)
	require.ErrorIs(t, d.RemoveAll(path.MustNewComponent("missing")), syscall.EBADF)
}

func TestLocalDirectoryRemovalPermissionDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "file"), nil, 0o600))
	directory, err := NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, directory.Close()) })
	d := directory.(*localDirectory)
	var stat syscall.Stat_t
	require.NoError(t, syscall.Fstat(d.fd, &stat))
	require.NoError(t, os.Chmod(root, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(root, 0o700)) })

	require.True(t, os.IsPermission(d.removeChildren(rawDeviceNumber(stat.Dev), []string{"file"})))
	require.True(t, os.IsPermission(d.RemoveAllChildren()))
	require.True(t, os.IsPermission(d.RemoveAll(path.MustNewComponent("file"))))
	_, err = os.Stat(filepath.Join(root, "file"))
	require.NoError(t, err)
}
