package filesystem_test

import (
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/stretchr/testify/require"
)

func openTmpDir(t *testing.T) filesystem.DirectoryCloser {
	d, err := filesystem.NewLocalDirectory(t.TempDir())
	require.NoError(t, err)
	return d
}

func TestLocalDirectoryCreationFailure(t *testing.T) {
	_, err := filesystem.NewLocalDirectory("/nonexistent")
	require.True(t, os.IsNotExist(err))
}

func TestLocalDirectoryCreationSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.EnterDirectory(path.MustNewComponent("nonexistent"))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = d.EnterDirectory(path.MustNewComponent("file"))
	require.Equal(t, syscall.ENOTDIR, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", path.MustNewComponent("symlink")))
	_, err := d.EnterDirectory(path.MustNewComponent("symlink"))
	require.Equal(t, syscall.ENOTDIR, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("subdir"), 0o777))
	sub, err := d.EnterDirectory(path.MustNewComponent("subdir"))
	require.NoError(t, err)
	require.NoError(t, sub.Close())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkNotFound(t *testing.T) {
	d := openTmpDir(t)
	require.Equal(t, syscall.ENOENT, d.Link(path.MustNewComponent("source"), d, path.MustNewComponent("target")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("source"), 0o777))
	require.True(t, os.IsPermission(d.Link(path.MustNewComponent("source"), d, path.MustNewComponent("target"))))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkTargetExists(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("source"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	f, err = d.OpenWrite(path.MustNewComponent("target"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, os.IsExist(d.Link(path.MustNewComponent("source"), d, path.MustNewComponent("target"))))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("source"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Link(path.MustNewComponent("source"), d, path.MustNewComponent("target")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.Lstat(path.MustNewComponent("hello"))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o644))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	fi, err := d.Lstat(path.MustNewComponent("file"))
	require.NoError(t, err)
	require.Equal(t, path.MustNewComponent("file"), fi.Name())
	require.Equal(t, filesystem.FileTypeRegularFile, fi.Type())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", path.MustNewComponent("symlink")))
	fi, err := d.Lstat(path.MustNewComponent("symlink"))
	require.NoError(t, err)
	require.Equal(t, path.MustNewComponent("symlink"), fi.Name())
	require.Equal(t, filesystem.FileTypeSymlink, fi.Type())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("directory"), 0o700))
	fi, err := d.Lstat(path.MustNewComponent("directory"))
	require.NoError(t, err)
	require.Equal(t, path.MustNewComponent("directory"), fi.Name())
	require.Equal(t, filesystem.FileTypeDirectory, fi.Type())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryMkdirExisting(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", path.MustNewComponent("symlink")))
	require.True(t, os.IsExist(d.Mkdir(path.MustNewComponent("symlink"), 0o777)))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryMkdirSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("directory"), 0o777))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenWriteExistent(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.True(t, os.IsExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenReadNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.OpenRead(path.MustNewComponent("file"))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenReadSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/etc/passwd", path.MustNewComponent("symlink")))
	_, err := d.OpenRead(path.MustNewComponent("symlink"))
	require.Equal(t, syscall.ELOOP, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenWriteSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadDir(t *testing.T) {
	d := openTmpDir(t)

	// Prepare file system.
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Mkdir(path.MustNewComponent("directory"), 0o777))
	require.NoError(t, d.Symlink("/", path.MustNewComponent("symlink")))

	// Validate directory listing.
	files, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, 3, len(files))
	require.Equal(t, path.MustNewComponent("directory"), files[0].Name())
	require.Equal(t, filesystem.FileTypeDirectory, files[0].Type())
	require.Equal(t, path.MustNewComponent("file"), files[1].Name())
	require.Equal(t, filesystem.FileTypeRegularFile, files[1].Type())
	require.Equal(t, path.MustNewComponent("symlink"), files[2].Name())
	require.Equal(t, filesystem.FileTypeSymlink, files[2].Type())

	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.Readlink(path.MustNewComponent("nonexistent"))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("directory"), 0o777))
	_, err := d.Readlink(path.MustNewComponent("directory"))
	require.Equal(t, syscall.EINVAL, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = d.Readlink(path.MustNewComponent("file"))
	require.Equal(t, syscall.EINVAL, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/foo/bar/baz", path.MustNewComponent("symlink")))
	target, err := d.Readlink(path.MustNewComponent("symlink"))
	require.NoError(t, err)
	require.Equal(t, "/foo/bar/baz", target)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveNonExistent(t *testing.T) {
	d := openTmpDir(t)
	require.True(t, os.IsNotExist(d.Remove(path.MustNewComponent("nonexistent"))))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("directory"), 0o777))
	require.NoError(t, d.Remove(path.MustNewComponent("directory")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Remove(path.MustNewComponent("file")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", path.MustNewComponent("symlink")))
	require.NoError(t, d.Remove(path.MustNewComponent("symlink")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRenameNotFound(t *testing.T) {
	d := openTmpDir(t)
	require.True(t, os.IsNotExist(d.Rename(path.MustNewComponent("source"), d, path.MustNewComponent("target"))))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRenameSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite(path.MustNewComponent("source"), filesystem.CreateExcl(0o666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Rename(path.MustNewComponent("source"), d, path.MustNewComponent("target")))
	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkExistent(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("directory"), 0o777))
	require.True(t, os.IsExist(d.Symlink("/", path.MustNewComponent("directory"))))
	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", path.MustNewComponent("symlink")))
	require.NoError(t, d.Close())
}

func TestLocalDirectorySync(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Sync())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryChtimes(t *testing.T) {
	d := openTmpDir(t)
	time := filesystem.DeterministicFileModificationTimestamp
	f, err := d.OpenAppend(path.MustNewComponent("file"), filesystem.CreateExcl(0o444))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Chtimes(path.MustNewComponent("file"), time, time))
	require.NoError(t, d.Close())
}

// TODO(edsch): Add testing coverage for RemoveAll().

func TestLocalDirectoryFileGetDataRegionOffset(t *testing.T) {
	// Test the behavior on empty files.
	d := openTmpDir(t)
	f, err := d.OpenReadWrite(path.MustNewComponent("file"), filesystem.CreateExcl(0o444))
	require.NoError(t, err)

	_, err = f.GetNextRegionOffset(0, filesystem.Data)
	require.Equal(t, io.EOF, err)
	_, err = f.GetNextRegionOffset(0, filesystem.Hole)
	require.Equal(t, io.EOF, err)

	_, err = f.GetNextRegionOffset(1, filesystem.Data)
	require.Equal(t, io.EOF, err)
	_, err = f.GetNextRegionOffset(1, filesystem.Hole)
	require.Equal(t, io.EOF, err)

	// Test the behavior on a sparse file that starts with a hole
	// and ends with data.
	n, err := f.WriteAt([]byte("Hello"), 1024*1024)
	require.Equal(t, 5, n)
	require.NoError(t, err)

	nextOffset, err := f.GetNextRegionOffset(0, filesystem.Data)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024), nextOffset)
	nextOffset, err = f.GetNextRegionOffset(0, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(0), nextOffset)

	nextOffset, err = f.GetNextRegionOffset(1, filesystem.Data)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024), nextOffset)
	nextOffset, err = f.GetNextRegionOffset(1, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(1), nextOffset)

	nextOffset, err = f.GetNextRegionOffset(1024*1024-1, filesystem.Data)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024), nextOffset)
	nextOffset, err = f.GetNextRegionOffset(1024*1024-1, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024-1), nextOffset)

	nextOffset, err = f.GetNextRegionOffset(1024*1024, filesystem.Data)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024), nextOffset)
	nextOffset, err = f.GetNextRegionOffset(1024*1024, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024+5), nextOffset)

	nextOffset, err = f.GetNextRegionOffset(1024*1024+4, filesystem.Data)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024+4), nextOffset)
	nextOffset, err = f.GetNextRegionOffset(1024*1024+4, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(1024*1024+5), nextOffset)

	_, err = f.GetNextRegionOffset(1024*1024+5, filesystem.Data)
	require.Equal(t, io.EOF, err)
	_, err = f.GetNextRegionOffset(1024*1024+5, filesystem.Hole)
	require.Equal(t, io.EOF, err)

	// Test the behavior on a sparse file that ends with a hole.
	require.NoError(t, f.Truncate(3072*1024))

	_, err = f.GetNextRegionOffset(2048*1024, filesystem.Data)
	require.Equal(t, io.EOF, err)
	nextOffset, err = f.GetNextRegionOffset(2048*1024, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(2048*1024), nextOffset)

	_, err = f.GetNextRegionOffset(3072*1024-1, filesystem.Data)
	require.Equal(t, io.EOF, err)
	nextOffset, err = f.GetNextRegionOffset(3072*1024-1, filesystem.Hole)
	require.NoError(t, err)
	require.Equal(t, int64(3072*1024-1), nextOffset)

	_, err = f.GetNextRegionOffset(3072*1024, filesystem.Data)
	require.Equal(t, io.EOF, err)
	_, err = f.GetNextRegionOffset(3072*1024, filesystem.Hole)
	require.Equal(t, io.EOF, err)

	require.NoError(t, f.Close())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryIsWritable(t *testing.T) {
	d := openTmpDir(t)
	{
		isWritable, err := d.IsWritable()
		require.NoError(t, err)
		require.True(t, isWritable, "Want dir to be writable")
	}

	require.NoError(t, d.Mkdir(path.MustNewComponent("child"), 0o555))
	child, err := d.EnterDirectory(path.MustNewComponent("child"))
	require.NoError(t, err)
	defer child.Close()

	{
		isWritable, err := child.IsWritable()
		require.NoError(t, err)
		require.False(t, isWritable)
	}
}

func TestLocalDirectoryIsWritableChild(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir(path.MustNewComponent("subdir"), 0o555))
	{
		isWritable, err := d.IsWritableChild(path.MustNewComponent("subdir"))
		require.NoError(t, err)
		require.False(t, isWritable, "Want dir not to be writable")
	}

	writeFile(t, d, "writable_file", 0o777)
	{
		isWritable, err := d.IsWritableChild(path.MustNewComponent("writable_file"))
		require.NoError(t, err)
		require.True(t, isWritable, "Want file to be writable")
	}

	writeFile(t, d, "unwritable_file", 0o555)
	{
		isWritable, err := d.IsWritableChild(path.MustNewComponent("unwritable_file"))
		require.NoError(t, err)
		require.False(t, isWritable, "Want file not to be writable")
	}
}

func writeFile(t *testing.T, directory filesystem.Directory, name string, permissions os.FileMode) {
	f, err := directory.OpenWrite(path.MustNewComponent(name), filesystem.CreateExcl(permissions))
	require.NoError(t, err)
	f.Close()
}
