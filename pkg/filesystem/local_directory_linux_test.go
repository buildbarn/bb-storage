//go:build linux

package filesystem_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestLocalDirectoryMkdirSticky(t *testing.T) {
	root := t.TempDir()
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	for _, tc := range []struct {
		name string
		mode os.FileMode
	}{
		{name: "private", mode: 0o700},
		{name: "sticky", mode: 0o700 | os.ModeSticky},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, d.Mkdir(path.MustNewComponent(tc.name), tc.mode))
			info, err := os.Stat(filepath.Join(root, tc.name))
			require.NoError(t, err)
			require.Equal(t, os.ModeDir|tc.mode, info.Mode())
		})
	}
}

func TestLocalDirectoryMkdirStickyUmask(t *testing.T) {
	if value := os.Getenv("BB_STORAGE_TEST_UMASK"); value != "" {
		mask, err := strconv.ParseUint(value, 8, 32)
		require.NoError(t, err)
		root := t.TempDir()
		d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		// A subprocess isolates the process-wide umask from other tests.
		previous := syscall.Umask(int(mask))
		defer syscall.Umask(previous)
		require.NoError(t, d.Mkdir(path.MustNewComponent("created"), 0o777|os.ModeSticky))
		info, err := os.Stat(filepath.Join(root, "created"))
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|os.FileMode(0o777&^mask)|os.ModeSticky, info.Mode())
		return
	}
	for _, mask := range []string{"0", "022", "077", "0700", "0777"} {
		t.Run(mask, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestLocalDirectoryMkdirStickyUmask$")
			command.Env = append(os.Environ(), "BB_STORAGE_TEST_UMASK="+mask)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func TestLocalDirectoryMkdirStickyExisting(t *testing.T) {
	root := t.TempDir()
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	target := filepath.Join(root, "existing")
	require.NoError(t, os.Mkdir(target, 0o700))
	require.NoError(t, os.Symlink(target, filepath.Join(root, "symlink")))
	for _, name := range []string{"existing", "symlink"} {
		require.True(t, os.IsExist(d.Mkdir(path.MustNewComponent(name), 0o777|os.ModeSticky)))
	}
	info, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|os.FileMode(0o700), info.Mode())
	link, err := os.Readlink(filepath.Join(root, "symlink"))
	require.NoError(t, err)
	require.Equal(t, target, link)
}
