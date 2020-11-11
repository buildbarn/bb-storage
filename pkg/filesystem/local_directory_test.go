package filesystem_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func openTmpDir(t *testing.T) filesystem.DirectoryCloser {
	p := filepath.Join(os.Getenv("TEST_TMPDIR"), t.Name())
	require.NoError(t, os.Mkdir(p, 0777))
	d, err := filesystem.NewLocalDirectory(p)
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

func TestLocalDirectoryEnterBadName(t *testing.T) {
	d := openTmpDir(t)

	// Empty filename.
	_, err := d.EnterDirectory("")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	// Attempt to bypass directory hierarchy.
	_, err = d.EnterDirectory(".")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.EnterDirectory("..")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	// Skipping of intermediate directory levels.
	_, err = d.EnterDirectory("foo/bar")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)

	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.EnterDirectory("nonexistent")
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = d.EnterDirectory("file")
	require.Equal(t, syscall.ENOTDIR, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", "symlink"))
	_, err := d.EnterDirectory("symlink")
	require.Equal(t, syscall.ENOTDIR, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryEnterSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("subdir", 0777))
	sub, err := d.EnterDirectory("subdir")
	require.NoError(t, err)
	require.NoError(t, sub.Close())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkBadName(t *testing.T) {
	d := openTmpDir(t)

	// Invalid source name.
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Link("", d, "file"))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Link(".", d, "file"))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Link("..", d, "file"))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Link("foo/bar", d, "file"))

	// Invalid target name.
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Link("file", d, ""))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Link("file", d, "."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Link("file", d, ".."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Link("file", d, "foo/bar"))

	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkNotFound(t *testing.T) {
	d := openTmpDir(t)
	require.Equal(t, syscall.ENOENT, d.Link("source", d, "target"))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("source", 0777))
	require.True(t, os.IsPermission(d.Link("source", d, "target")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkTargetExists(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("source", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	f, err = d.OpenWrite("target", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, os.IsExist(d.Link("source", d, "target")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLinkSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("source", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Link("source", d, "target"))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatBadName(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.Lstat("")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	_, err = d.Lstat(".")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.Lstat("..")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	_, err = d.Lstat("foo/bar")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.Lstat("hello")
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0644))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	fi, err := d.Lstat("file")
	require.NoError(t, err)
	require.Equal(t, "file", fi.Name())
	require.Equal(t, filesystem.FileTypeRegularFile, fi.Type())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", "symlink"))
	fi, err := d.Lstat("symlink")
	require.NoError(t, err)
	require.Equal(t, "symlink", fi.Name())
	require.Equal(t, filesystem.FileTypeSymlink, fi.Type())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryLstatDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("directory", 0700))
	fi, err := d.Lstat("directory")
	require.NoError(t, err)
	require.Equal(t, "directory", fi.Name())
	require.Equal(t, filesystem.FileTypeDirectory, fi.Type())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryMkdirBadName(t *testing.T) {
	d := openTmpDir(t)
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Mkdir("", 0777))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Mkdir(".", 0777))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Mkdir("..", 0777))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Mkdir("foo/bar", 0777))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryMkdirExisting(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", "symlink"))
	require.True(t, os.IsExist(d.Mkdir("symlink", 0777)))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryMkdirSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("directory", 0777))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenWriteBadName(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.OpenWrite("", filesystem.CreateExcl(0666))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	_, err = d.OpenWrite(".", filesystem.CreateExcl(0666))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.OpenWrite("..", filesystem.CreateExcl(0666))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	_, err = d.OpenWrite("foo/bar", filesystem.CreateExcl(0666))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenWriteExistent(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.True(t, os.IsExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenReadNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.OpenRead("file")
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenReadSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/etc/passwd", "symlink"))
	_, err := d.OpenRead("symlink")
	require.Equal(t, syscall.ELOOP, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryOpenWriteSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadDir(t *testing.T) {
	d := openTmpDir(t)

	// Prepare file system.
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Mkdir("directory", 0777))
	require.NoError(t, d.Symlink("/", "symlink"))

	// Validate directory listing.
	files, err := d.ReadDir()
	require.NoError(t, err)
	require.Equal(t, 3, len(files))
	require.Equal(t, "directory", files[0].Name())
	require.Equal(t, filesystem.FileTypeDirectory, files[0].Type())
	require.Equal(t, "file", files[1].Name())
	require.Equal(t, filesystem.FileTypeRegularFile, files[1].Type())
	require.Equal(t, "symlink", files[2].Name())
	require.Equal(t, filesystem.FileTypeSymlink, files[2].Type())

	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkBadName(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.Readlink("")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), err)
	_, err = d.Readlink(".")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), err)
	_, err = d.Readlink("..")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), err)
	_, err = d.Readlink("foo/bar")
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkNonExistent(t *testing.T) {
	d := openTmpDir(t)
	_, err := d.Readlink("nonexistent")
	require.True(t, os.IsNotExist(err))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("directory", 0777))
	_, err := d.Readlink("directory")
	require.Equal(t, syscall.EINVAL, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, err = d.Readlink("file")
	require.Equal(t, syscall.EINVAL, err)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryReadlinkSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/foo/bar/baz", "symlink"))
	target, err := d.Readlink("symlink")
	require.NoError(t, err)
	require.Equal(t, "/foo/bar/baz", target)
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveBadName(t *testing.T) {
	d := openTmpDir(t)
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Remove(""))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Remove("."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Remove(".."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Remove("foo/bar"))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveNonExistent(t *testing.T) {
	d := openTmpDir(t)
	require.True(t, os.IsNotExist(d.Remove("nonexistent")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveDirectory(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("directory", 0777))
	require.NoError(t, d.Remove("directory"))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveFile(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("file", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Remove("file"))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRemoveSymlink(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", "symlink"))
	require.NoError(t, d.Remove("symlink"))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRenameBadName(t *testing.T) {
	d := openTmpDir(t)

	// Invalid source name.
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Rename("", d, "file"))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Rename(".", d, "file"))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Rename("..", d, "file"))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Rename("foo/bar", d, "file"))

	// Invalid target name.
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Rename("file", d, ""))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Rename("file", d, "."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Rename("file", d, ".."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Rename("file", d, "foo/bar"))

	require.NoError(t, d.Close())
}

func TestLocalDirectoryRenameNotFound(t *testing.T) {
	d := openTmpDir(t)
	require.True(t, os.IsNotExist(d.Rename("source", d, "target")))
	require.NoError(t, d.Close())
}

func TestLocalDirectoryRenameSuccess(t *testing.T) {
	d := openTmpDir(t)
	f, err := d.OpenWrite("source", filesystem.CreateExcl(0666))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Rename("source", d, "target"))
	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkBadName(t *testing.T) {
	d := openTmpDir(t)
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"\""), d.Symlink("/whatever", ""))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \".\""), d.Symlink("/whatever", "."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"..\""), d.Symlink("/whatever", ".."))
	require.Equal(t, status.Error(codes.InvalidArgument, "Invalid filename: \"foo/bar\""), d.Symlink("/whatever", "foo/bar"))
	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkExistent(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Mkdir("directory", 0777))
	require.True(t, os.IsExist(d.Symlink("/", "directory")))
	require.NoError(t, d.Close())
}

func TestLocalDirectorySymlinkSuccess(t *testing.T) {
	d := openTmpDir(t)
	require.NoError(t, d.Symlink("/", "symlink"))
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
	f, err := d.OpenAppend("file", filesystem.CreateExcl(0444))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, d.Chtimes("file", time, time))
	require.NoError(t, d.Close())
}

// TODO(edsch): Add testing coverage for RemoveAll().

func TestLocalDirectoryIsWritable(t *testing.T) {
	d := openTmpDir(t)
	{
		isWritable, err := d.IsWritable()
		require.NoError(t, err)
		require.True(t, isWritable, "Want dir to be writable")
	}

	require.NoError(t, d.Mkdir("child", 0o555))
	child, err := d.EnterDirectory("child")
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
	require.NoError(t, d.Mkdir("subdir", 0o555))
	{
		isWritable, err := d.IsWritableChild("subdir")
		require.NoError(t, err)
		require.False(t, isWritable, "Want dir not to be writable")
	}

	writeFile(t, d, "writable_file", 0o777)
	{
		isWritable, err := d.IsWritableChild("writable_file")
		require.NoError(t, err)
		require.True(t, isWritable, "Want file to be writable")
	}

	writeFile(t, d, "unwritable_file", 0o555)
	{
		isWritable, err := d.IsWritableChild("unwritable_file")
		require.NoError(t, err)
		require.False(t, isWritable, "Want file not to be writable")
	}
}

func writeFile(t *testing.T, directory filesystem.Directory, name string, permissions os.FileMode) {
	f, err := directory.OpenWrite(name, filesystem.CreateExcl(permissions))
	require.NoError(t, err)
	f.Close()
}
