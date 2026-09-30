//go:build darwin || freebsd

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
	for _, mode := range []os.FileMode{0o777, 0o700, 0o111, 0o100, 0o400, 0} {
		for _, sticky := range []os.FileMode{0, os.ModeSticky} {
			t.Run((mode | sticky).String(), func(t *testing.T) {
				root := t.TempDir()
				d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, d.Close()) })
				// An ordinary directory records the kernel's umask and
				// inherited permissions without changing the process umask.
				require.NoError(t, os.Mkdir(filepath.Join(root, "reference"), mode))
				reference, err := os.Stat(filepath.Join(root, "reference"))
				require.NoError(t, err)
				want := reference.Mode() & (os.ModePerm | os.ModeSetuid | os.ModeSetgid)
				created := filepath.Join(root, "created")
				mkdirErr := d.Mkdir(path.MustNewComponent("created"), mode|sticky)
				t.Cleanup(func() { require.NoError(t, os.Chmod(created, 0o700)) })
				if sticky != 0 && os.Geteuid() != 0 && want&0o100 == 0 {
					// Installing the bit requires opening the new directory
					// for search. Failure leaves the created entry in place.
					require.ErrorIs(t, mkdirErr, syscall.EACCES)
				} else {
					require.NoError(t, mkdirErr)
					want |= sticky
				}
				info, err := os.Stat(created)
				require.NoError(t, err)
				require.Equal(t, os.ModeDir|want, info.Mode())
			})
		}
	}
}

func TestLocalDirectoryMkdirStickyUmask(t *testing.T) {
	if value := os.Getenv("BB_STORAGE_TEST_BSD_UMASK"); value != "" {
		mask, err := strconv.ParseUint(value, 8, 32)
		require.NoError(t, err)
		root := t.TempDir()
		d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		previous := syscall.Umask(int(mask))
		defer syscall.Umask(previous)
		mkdirErr := d.Mkdir(path.MustNewComponent("created"), 0o777|os.ModeSticky)
		created := filepath.Join(root, "created")
		t.Cleanup(func() { require.NoError(t, os.Chmod(created, 0o700)) })
		want := os.FileMode(0o777 &^ mask)
		if os.Geteuid() != 0 && mask&0o100 != 0 {
			require.ErrorIs(t, mkdirErr, syscall.EACCES)
		} else {
			require.NoError(t, mkdirErr)
			want |= os.ModeSticky
		}
		info, err := os.Stat(created)
		require.NoError(t, err)
		require.Equal(t, os.ModeDir|want, info.Mode())
		return
	}
	for _, mask := range []string{"0", "022", "077", "0400", "0100", "0700", "0777"} {
		t.Run(mask, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestLocalDirectoryMkdirStickyUmask$")
			command.Env = append(os.Environ(), "BB_STORAGE_TEST_BSD_UMASK="+mask)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}

func TestLocalDirectoryMkdirStickyWritableParent(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Chmod(root, 0o777))
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	require.NoError(t, d.Mkdir(path.MustNewComponent("created"), 0o700|os.ModeSticky))
	info, err := os.Stat(filepath.Join(root, "created"))
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|os.ModeSticky|0o700, info.Mode())
}

func TestLocalDirectoryMkdirStickyInheritedPermissions(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Chmod(root, os.ModeSetgid|0o700))
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	require.NoError(t, os.Mkdir(filepath.Join(root, "reference"), 0o700))
	reference, err := os.Stat(filepath.Join(root, "reference"))
	require.NoError(t, err)
	require.NoError(t, d.Mkdir(path.MustNewComponent("created"), 0o700|os.ModeSticky))
	info, err := os.Stat(filepath.Join(root, "created"))
	require.NoError(t, err)
	require.Equal(t, reference.Mode()|os.ModeSticky, info.Mode())
	require.Equal(t, reference.Sys().(*syscall.Stat_t).Gid, info.Sys().(*syscall.Stat_t).Gid)
}

func TestLocalDirectoryMkdirStickyExisting(t *testing.T) {
	root := t.TempDir()
	d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, d.Close()) })
	target := filepath.Join(t.TempDir(), "outside")
	require.NoError(t, os.WriteFile(target, []byte("unchanged"), 0o400))
	require.NoError(t, os.Link(target, filepath.Join(root, "hardlink")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "file"), []byte("file"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(root, "directory"), 0o700))
	require.NoError(t, os.Symlink(target, filepath.Join(root, "symlink")))
	for _, name := range []string{"directory", "file", "hardlink", "symlink"} {
		before, err := os.Lstat(filepath.Join(root, name))
		require.NoError(t, err)
		require.ErrorIs(t, d.Mkdir(path.MustNewComponent(name), 0o777|os.ModeSticky), syscall.EEXIST)
		after, err := os.Lstat(filepath.Join(root, name))
		require.NoError(t, err)
		require.Equal(t, before.Mode(), after.Mode())
		require.True(t, os.SameFile(before, after))
	}
	info, err := os.Stat(target)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o400), info.Mode())
	data, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, "unchanged", string(data))
	link, err := os.Readlink(filepath.Join(root, "symlink"))
	require.NoError(t, err)
	require.Equal(t, target, link)
}

func TestLocalDirectoryMkdirStickyDescriptorLifetime(t *testing.T) {
	if value := os.Getenv("BB_STORAGE_TEST_FD_BUDGET"); value != "" {
		spare, err := strconv.Atoi(value)
		require.NoError(t, err)
		root := t.TempDir()
		d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, d.Close()) })
		var original syscall.Rlimit
		require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &original))
		limited := original
		limited.Cur = 64
		var descriptors []int
		defer func() {
			for _, fd := range descriptors {
				_ = syscall.Close(fd)
			}
			require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original))
		}()
		require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limited))
		for {
			fd, err := syscall.Open("/dev/null", syscall.O_RDONLY, 0)
			if err != nil {
				require.ErrorIs(t, err, syscall.EMFILE)
				break
			}
			descriptors = append(descriptors, fd)
		}
		for range spare {
			require.NoError(t, syscall.Close(descriptors[len(descriptors)-1]))
			descriptors = descriptors[:len(descriptors)-1]
		}
		mkdirErr := d.Mkdir(path.MustNewComponent("created"), 0o700|os.ModeSticky)
		available := 0
		for {
			fd, err := syscall.Open("/dev/null", syscall.O_RDONLY, 0)
			if err != nil {
				require.ErrorIs(t, err, syscall.EMFILE)
				break
			}
			available++
			descriptors = append(descriptors, fd)
		}
		for _, fd := range descriptors {
			require.NoError(t, syscall.Close(fd))
		}
		descriptors = nil
		require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original))
		require.Equal(t, spare, available, "Mkdir leaked a descriptor")
		want := os.ModeDir | os.FileMode(0o700)
		if spare == 0 {
			require.ErrorIs(t, mkdirErr, syscall.EMFILE)
		} else {
			require.NoError(t, mkdirErr)
			want |= os.ModeSticky
		}
		info, err := os.Stat(filepath.Join(root, "created"))
		require.NoError(t, err)
		require.Equal(t, want, info.Mode())
		return
	}
	for _, spare := range []string{"0", "1"} {
		t.Run(spare, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestLocalDirectoryMkdirStickyDescriptorLifetime$")
			command.Env = append(os.Environ(), "BB_STORAGE_TEST_FD_BUDGET="+spare)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}
