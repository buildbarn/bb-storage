//go:build darwin || freebsd || linux

package filesystem_test

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func TestLocalDirectoryRemoveAllChildrenConcurrent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	marker := filepath.Join(outside, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("untouched"), 0o600))
	var children []path.Component
	for i := range 16 {
		name := strconv.Itoa(i)
		children = append(children, path.MustNewComponent(name))
		require.NoError(t, os.Mkdir(filepath.Join(root, name), 0o700))
		require.NoError(t, os.WriteFile(filepath.Join(root, name, "file"), nil, 0o600))
	}
	// Only the full sweep removes these siblings; the other callers remove
	// the numbered children. Cleanup must finish without following the link.
	require.NoError(t, os.WriteFile(filepath.Join(root, "surviving"), nil, 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(root, "outside")))

	var directories [5]filesystem.DirectoryCloser
	for i := range directories {
		d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		directories[i] = d
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var errors [len(directories)]error
	for i, d := range directories {
		wg.Go(func() {
			<-start
			if i == 0 {
				errors[i] = d.RemoveAllChildren()
				return
			}
			for _, child := range children {
				if err := d.RemoveAll(child); err != nil {
					errors[i] = err
					return
				}
			}
		})
	}
	close(start)
	wg.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
}

func TestLocalDirectoryRemoveAllMissing(t *testing.T) {
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	require.NoError(t, d.RemoveAll(path.MustNewComponent("missing")))
}

func TestLocalDirectoryRemovalClosed(t *testing.T) {
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, d.Close())
	require.ErrorIs(t, d.RemoveAllChildren(), syscall.EBADF)
	require.ErrorIs(t, d.RemoveAll(path.MustNewComponent("missing")), syscall.EBADF)
}

func TestLocalDirectoryRemovalPermissionDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory write permissions")
	}
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "file"), nil, 0o600))
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	require.NoError(t, os.Chmod(root, 0o500))
	t.Cleanup(func() { require.NoError(t, os.Chmod(root, 0o700)) })

	require.True(t, os.IsPermission(d.RemoveAllChildren()))
	require.True(t, os.IsPermission(d.RemoveAll(path.MustNewComponent("file"))))
	_, err = os.Stat(filepath.Join(root, "file"))
	require.NoError(t, err)
}
