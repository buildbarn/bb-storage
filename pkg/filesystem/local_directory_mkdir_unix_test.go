//go:build darwin || freebsd || linux

package filesystem_test

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
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
				if sticky != 0 && runtime.GOOS != "linux" && runtime.GOOS != "android" && os.Geteuid() != 0 && want&0o100 == 0 {
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
	// These cases change process state and must remain sequential.
	for _, mask := range []int{0, 0o022, 0o077, 0o400, 0o100, 0o700, 0o777} {
		t.Run(strconv.FormatInt(int64(mask), 8), func(t *testing.T) {
			root := t.TempDir()
			d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			created := filepath.Join(root, "created")
			name := path.MustNewComponent("created")
			mkdirErr := func() error {
				previous := syscall.Umask(mask)
				defer syscall.Umask(previous)
				return d.Mkdir(name, 0o777|os.ModeSticky)
			}()
			t.Cleanup(func() { require.NoError(t, os.Chmod(created, 0o700)) })
			want := os.FileMode(0o777 &^ mask)
			if runtime.GOOS != "linux" && runtime.GOOS != "android" && os.Geteuid() != 0 && mask&0o100 != 0 {
				require.ErrorIs(t, mkdirErr, syscall.EACCES)
			} else {
				require.NoError(t, mkdirErr)
				want |= os.ModeSticky
			}
			info, err := os.Stat(created)
			require.NoError(t, err)
			require.Equal(t, os.ModeDir|want, info.Mode())
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
	require.NoError(t, os.Symlink(filepath.Join(root, "directory"), filepath.Join(root, "directory_symlink")))
	for _, name := range []string{"directory", "file", "hardlink", "symlink", "directory_symlink"} {
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
	info, err = os.Stat(filepath.Join(root, "directory"))
	require.NoError(t, err)
	require.Equal(t, os.ModeDir|0o700, info.Mode())
	link, err = os.Readlink(filepath.Join(root, "directory_symlink"))
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "directory"), link)
}

func TestLocalDirectoryMkdirStickyDescriptorLifetime(t *testing.T) {
	// These cases change process state and must remain sequential.
	for _, spare := range []int{0, 1} {
		t.Run(strconv.Itoa(spare), func(t *testing.T) {
			root := t.TempDir()
			d, err := filesystem.NewLocalDirectory(path.LocalFormat.NewParser(root))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, d.Close()) })
			name := path.MustNewComponent("created")
			var original syscall.Rlimit
			require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_NOFILE, &original))
			limited := original
			limited.Cur = min(limited.Cur, 64)
			descriptors := make([]int, 0, limited.Cur+1)
			var fillErr, mkdirErr, probeErr, cleanupErr error
			var opened, available int
			operationErr := func() error {
				defer func() {
					cleanupErr = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &original)
					for _, fd := range descriptors {
						cleanupErr = errors.Join(cleanupErr, syscall.Close(fd))
					}
				}()
				if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limited); err != nil {
					return err
				}
				// Do not report failures until the original limit is restored.
				for range limited.Cur + 1 {
					fd, err := syscall.Open("/dev/null", syscall.O_RDONLY, 0)
					if err != nil {
						fillErr = err
						break
					}
					descriptors = append(descriptors, fd)
				}
				opened = len(descriptors)
				if fillErr != syscall.EMFILE || opened < spare {
					return nil
				}
				for range spare {
					if err := syscall.Close(descriptors[len(descriptors)-1]); err != nil {
						return err
					}
					descriptors = descriptors[:len(descriptors)-1]
				}
				mkdirErr = d.Mkdir(name, 0o700|os.ModeSticky)
				for range limited.Cur + 1 {
					fd, err := syscall.Open("/dev/null", syscall.O_RDONLY, 0)
					if err != nil {
						probeErr = err
						break
					}
					available++
					descriptors = append(descriptors, fd)
				}
				return nil
			}()
			require.NoError(t, cleanupErr)
			require.NoError(t, operationErr)
			require.ErrorIs(t, fillErr, syscall.EMFILE)
			require.GreaterOrEqual(t, opened, spare)
			require.ErrorIs(t, probeErr, syscall.EMFILE)
			require.Equal(t, spare, available, "Mkdir leaked a descriptor")
			want := os.ModeDir | os.FileMode(0o700)
			if spare == 0 && runtime.GOOS != "linux" && runtime.GOOS != "android" {
				require.ErrorIs(t, mkdirErr, syscall.EMFILE)
			} else {
				require.NoError(t, mkdirErr)
				want |= os.ModeSticky
			}
			info, err := os.Stat(filepath.Join(root, "created"))
			require.NoError(t, err)
			require.Equal(t, want, info.Mode())
		})
	}
}
