package filesystem

import (
	"errors"
	"os"
	"runtime"
	"sort"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type localDirectory struct {
	fd int
}

func validateFilename(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') {
		return status.Errorf(codes.InvalidArgument, "Invalid filename: %#v", name)
	}
	return nil
}

// NewLocalDirectory creates a directory handle that corresponds to a
// local path on the system.
func NewLocalDirectory(path string) (Directory, error) {
	fd, err := unix.Openat(unix.AT_FDCWD, path, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	d := &localDirectory{
		fd: fd,
	}
	runtime.SetFinalizer(d, (*localDirectory).Close)
	return d, nil
}

func (d *localDirectory) Enter(name string) (Directory, error) {
	if err := validateFilename(name); err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(d)

	fd, err := unix.Openat(d.fd, name, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	cd := &localDirectory{
		fd: fd,
	}
	runtime.SetFinalizer(cd, (*localDirectory).Close)
	return cd, nil
}

func (d *localDirectory) Close() error {
	fd := d.fd
	d.fd = -1
	runtime.SetFinalizer(d, nil)
	return unix.Close(fd)
}

func (d *localDirectory) Link(oldName string, newDirectory Directory, newName string) error {
	if err := validateFilename(oldName); err != nil {
		return err
	}
	if err := validateFilename(newName); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)
	defer runtime.KeepAlive(newDirectory)

	d2, ok := newDirectory.(*localDirectory)
	if !ok {
		return errors.New("Source and target directory have different types")
	}
	return unix.Linkat(d.fd, oldName, d2.fd, newName, 0)
}

func (d *localDirectory) Lstat(name string) (FileInfo, error) {
	if err := validateFilename(name); err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(d)

	var stat unix.Stat_t
	err := unix.Fstatat(d.fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW)
	if err != nil {
		return nil, err
	}
	mode := os.FileMode(stat.Mode & 0777)
	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		mode |= os.ModeDir
	case syscall.S_IFLNK:
		mode |= os.ModeSymlink
	case syscall.S_IFREG:
		// Regular files have a mode of zero.
	default:
		mode |= os.ModeIrregular
	}
	return NewSimpleFileInfo(name, mode), nil
}

func (d *localDirectory) Mkdir(name string, perm os.FileMode) error {
	if err := validateFilename(name); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)

	return unix.Mkdirat(d.fd, name, uint32(perm))
}

func (d *localDirectory) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	if err := validateFilename(name); err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(d)

	fd, err := unix.Openat(d.fd, name, flag|unix.O_NOFOLLOW, uint32(perm))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (d *localDirectory) ReadDir() ([]FileInfo, error) {
	defer runtime.KeepAlive(d)

	// Obtain filenames in current directory.
	fd, err := unix.Openat(d.fd, ".", unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), ".")
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	// Obtain file info.
	var list []FileInfo
	for _, name := range names {
		info, err := d.Lstat(name)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		list = append(list, info)
	}
	return list, nil
}

func (d *localDirectory) Readlink(name string) (string, error) {
	if err := validateFilename(name); err != nil {
		return "", err
	}
	defer runtime.KeepAlive(d)

	for l := 128; ; l *= 2 {
		b := make([]byte, l)
		n, err := unix.Readlinkat(d.fd, name, b)
		if err != nil {
			return "", err
		}
		if n < l {
			return string(b[0:n]), nil
		}
	}
}

func (d *localDirectory) Remove(name string) error {
	if err := validateFilename(name); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)

	// First try deleting it as a regular file.
	err1 := unix.Unlinkat(d.fd, name, 0)
	if err1 == nil {
		return nil
	}
	// Then try to delete it as a directory.
	err2 := unix.Unlinkat(d.fd, name, unix.AT_REMOVEDIR)
	if err2 == nil {
		return nil
	}
	// Determine which error to return.
	if err1 != syscall.ENOTDIR {
		return err1
	}
	return err2
}

func (d *localDirectory) RemoveAllChildren() error {
	defer runtime.KeepAlive(d)

	children, err := d.ReadDir()
	if err != nil {
		return err
	}
	for _, child := range children {
		name := child.Name()
		if child.Mode()&os.ModeType == os.ModeDir {
			// A directory. Remove all children. Adjust permissions
			// to ensure we can delete directories with degenerate
			// permissions.
			// TODO(edsch): This could use AT_SYMLINK_NOFOLLOW.
			// Unfortunately, this is broken on Linux.
			// Details: https://github.com/golang/go/issues/20130
			unix.Fchmodat(d.fd, name, 0700, 0)
			subdirectory, err := d.Enter(name)
			if err != nil {
				return err
			}
			err = subdirectory.RemoveAllChildren()
			subdirectory.Close()
			if err != nil {
				return err
			}
			if err := unix.Unlinkat(d.fd, name, unix.AT_REMOVEDIR); err != nil {
				return err
			}
		} else {
			// Not a directory. Remove it immediately.
			if err := unix.Unlinkat(d.fd, name, 0); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *localDirectory) RemoveAll(name string) error {
	if err := validateFilename(name); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)

	// TODO(edsch): Call chmod(700) to ensure directory can be accessed?
	if subdirectory, err := d.Enter(name); err == nil {
		// A directory. Remove all children.
		err := subdirectory.RemoveAllChildren()
		subdirectory.Close()
		if err != nil {
			return err
		}
		return unix.Unlinkat(d.fd, name, unix.AT_REMOVEDIR)
	} else if err == syscall.ENOTDIR {
		// Not a directory. Remove it immediately.
		return unix.Unlinkat(d.fd, name, 0)
	} else {
		return err
	}
}

func (d *localDirectory) Symlink(oldName string, newName string) error {
	if err := validateFilename(newName); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)

	return unix.Symlinkat(oldName, d.fd, newName)
}
