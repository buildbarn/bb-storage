//go:build windows
// +build windows

package filesystem_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestLocalDirectorySymlinkWindowsRelative(t *testing.T) {
	// Test symlinks to targets with WindowsPathKindRelative.

	tempDir := t.TempDir()
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(tempDir))
	require.NoError(t, err)

	// Check WindowsPathKind.
	targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.WindowsFormat.NewParser("target_file"), scopeWalker))
	require.Equal(t, path.WindowsPathKindRelative, targetPath.WindowsPathKind())

	// Create symlink targets.
	f, err := d.OpenWrite(path.MustNewComponent("target_file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Mkdir(path.MustNewComponent("target_dir"), 0o777))

	// Create symlinks.
	require.NoError(t, d.Symlink(path.WindowsFormat.NewParser("target_file"), path.MustNewComponent("symlink_to_file")))
	require.NoError(t, d.Symlink(path.WindowsFormat.NewParser("target_dir"), path.MustNewComponent("symlink_to_dir")))

	// Verify symlinks.
	fi, err := d.Lstat(path.MustNewComponent("symlink_to_file"))
	require.NoError(t, err)
	require.Equal(t, filesystem.FileTypeSymlink, fi.Type())
	fi, err = d.Lstat(path.MustNewComponent("symlink_to_dir"))
	require.NoError(t, err)
	require.Equal(t, filesystem.FileTypeSymlink, fi.Type())

	// Verify symlinks can be read using standard Go filesystem routines.
	target, err := os.Readlink(filepath.Join(tempDir, "symlink_to_file"))
	require.NoError(t, err)
	require.Equal(t, "target_file", target)

	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkWindowsAbsolute(t *testing.T) {
	// Test symlinks to targets with WindowsPathKindAbsolute.
	tempDir := t.TempDir()
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(tempDir))
	require.NoError(t, err)

	targetDirName := "absolute_target"

	// Validate WindowsPathKind.
	targetDirPath := filepath.Join(tempDir, targetDirName)
	builder, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.WindowsFormat.NewParser(targetDirPath), scopeWalker))
	require.Equal(t, path.WindowsPathKindAbsolute, builder.WindowsPathKind())
	resolved, err := builder.GetWindowsString(path.WindowsPathFormatDevicePath)
	require.NoError(t, err)
	require.Equal(t, `\??\`+targetDirPath, resolved)

	// Create symlink targets and symlinks.
	require.NoError(t, d.Mkdir(path.MustNewComponent(targetDirName), 0o777))
	require.NoError(t, d.Symlink(path.WindowsFormat.NewParser(targetDirPath), path.MustNewComponent("symlink_to_absolute")))

	// Verify the symlink.
	fi, err := d.Lstat(path.MustNewComponent("symlink_to_absolute"))
	require.NoError(t, err)
	require.Equal(t, filesystem.FileTypeSymlink, fi.Type())
	targetParser, err := d.Readlink(path.MustNewComponent("symlink_to_absolute"))
	require.NoError(t, err)
	targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(targetParser, scopeWalker))
	targetString, err := targetPath.GetWindowsString(path.WindowsPathFormatStandard)
	require.NoError(t, err)
	require.Equal(t, targetDirPath, targetString)

	// Verify symlink can be read using standard Go filesystem routines.
	target, err := os.Readlink(filepath.Join(tempDir, "symlink_to_absolute"))
	require.NoError(t, err)
	require.Equal(t, targetDirPath, target)

	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkWindowsDriveRelative(t *testing.T) {
	// Test symlinks to targets with WindowsPathKindDriveRelative.
	tmpDir := t.TempDir()
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(tmpDir))
	require.NoError(t, err)

	// Chop the drive letter off.
	driveRelativePath := tmpDir[2:]

	// Validate WindowsPathKind for drive-relative path.
	targetPath, scopeWalker := path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(path.WindowsFormat.NewParser(driveRelativePath), scopeWalker))
	require.Equal(t, path.WindowsPathKindDriveRelative, targetPath.WindowsPathKind())

	// Create symlink.
	require.NoError(t, d.Symlink(path.WindowsFormat.NewParser(driveRelativePath), path.MustNewComponent("symlink_drive_relative")))

	fi, err := d.Lstat(path.MustNewComponent("symlink_drive_relative"))
	require.NoError(t, err)
	require.Equal(t, filesystem.FileTypeSymlink, fi.Type())

	// Validate the symlink target.
	targetParser, err := d.Readlink(path.MustNewComponent("symlink_drive_relative"))
	require.NoError(t, err)
	targetPath, scopeWalker = path.EmptyBuilder.Join(path.VoidScopeWalker)
	require.NoError(t, path.Resolve(targetParser, scopeWalker))
	targetString, err := targetPath.GetWindowsString(path.WindowsPathFormatStandard)
	require.NoError(t, err)
	require.Equal(t, driveRelativePath, targetString)

	// Verify symlink can be read using standard Go filesystem routines.
	target, err := os.Readlink(filepath.Join(tmpDir, "symlink_drive_relative"))
	require.NoError(t, err)
	require.Equal(t, driveRelativePath, target)

	require.NoError(t, d.Close())
}

func TestLocalDirectoryNewLocalDirectoryDriveRelativePath(t *testing.T) {
	tempDir := t.TempDir()

	subdir := "test_subdir"
	subdirPath := filepath.Join(tempDir, subdir)
	require.NoError(t, os.Mkdir(subdirPath, 0o755))

	// Save the current working directory so we can restore it.
	originalWd, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(tempDir))

	// Test NewLocalDirectory with a drive-relative path.
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(subdirPath[2:]))
	require.NoError(t, err)
	require.NoError(t, d.Close())

	require.NoError(t, os.Chdir(originalWd))
}

var procGetProcessHandleCount = windows.NewLazySystemDLL("kernel32.dll").NewProc("GetProcessHandleCount")

func processHandleCount(t *testing.T) uint32 {
	var n uint32
	r, _, err := procGetProcessHandleCount.Call(uintptr(windows.CurrentProcess()), uintptr(unsafe.Pointer(&n)))
	require.NotZero(t, r, err)
	return n
}

func TestLocalDirectoryWindowsNoHandleLeak(t *testing.T) {
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(t.TempDir()))
	require.NoError(t, err)
	defer d.Close()

	// Warm up lazily created runtime handles before sampling.
	require.NoError(t, d.Mkdir(path.MustNewComponent("warmup"), 0o777))
	require.NoError(t, d.RemoveAll(path.MustNewComponent("warmup")))

	const iterations = 1000
	before := processHandleCount(t)
	for i := 0; i < iterations; i++ {
		dir := path.MustNewComponent("dir")
		require.NoError(t, d.Mkdir(dir, 0o777))
		sub, err := d.EnterDirectory(dir)
		require.NoError(t, err)
		require.NoError(t, sub.Mkdir(path.MustNewComponent("child"), 0o777))
		f, err := sub.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
		require.NoError(t, err)
		require.NoError(t, f.Close())
		require.NoError(t, sub.Link(path.MustNewComponent("file"), sub, path.MustNewComponent("link")))
		require.Equal(t, syscall.ENOTEMPTY, d.Remove(dir))
		require.NoError(t, sub.Close())
		require.NoError(t, d.RemoveAll(dir))
	}
	require.Less(t, int64(processHandleCount(t))-int64(before), int64(iterations/10))
}
