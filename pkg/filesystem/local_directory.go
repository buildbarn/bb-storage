package filesystem

import (
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
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

func newLocalDirectoryFromFileDescriptor(fd int) (*localDirectory, error) {
	d := &localDirectory{
		fd: fd,
	}
	runtime.SetFinalizer(d, (*localDirectory).Close)
	return d, nil
}

// NewLocalDirectory creates a directory handle that corresponds to a
// local path on the system.
func NewLocalDirectory(path string) (DirectoryCloser, error) {
	fd, err := unix.Openat(unix.AT_FDCWD, path, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return newLocalDirectoryFromFileDescriptor(fd)
}

func (d *localDirectory) enter(name string) (*localDirectory, error) {
	if err := validateFilename(name); err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(d)

	fd, err := unix.Openat(d.fd, name, unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_RDONLY, 0)
	if err != nil {
		if runtime.GOOS == "freebsd" && err == syscall.EMLINK {
			// FreeBSD erroneously returns EMLINK.
			return nil, syscall.ENOTDIR
		}
		return nil, err
	}
	return newLocalDirectoryFromFileDescriptor(fd)
}

func (d *localDirectory) EnterDirectory(name string) (DirectoryCloser, error) {
	return d.enter(name)
}

func (d *localDirectory) Close() error {
	fd := d.fd
	d.fd = -1
	runtime.SetFinalizer(d, nil)
	return unix.Close(fd)
}

func (d *localDirectory) open(name string, creationMode CreationMode, flag int) (*os.File, error) {
	if err := validateFilename(name); err != nil {
		return nil, err
	}
	defer runtime.KeepAlive(d)

	fd, err := unix.Openat(d.fd, name, flag|creationMode.flags|unix.O_NOFOLLOW, uint32(creationMode.permissions))
	if err != nil {
		if runtime.GOOS == "freebsd" && err == syscall.EMLINK {
			// FreeBSD erroneously returns EMLINK.
			return nil, syscall.ELOOP
		}
		return nil, err
	}
	return os.NewFile(uintptr(fd), name), nil
}

func (d *localDirectory) OpenAppend(name string, creationMode CreationMode) (FileAppender, error) {
	return d.open(name, creationMode, os.O_APPEND|os.O_WRONLY)
}

func (d *localDirectory) OpenRead(name string) (FileReader, error) {
	return d.open(name, DontCreate, os.O_RDONLY)
}

func (d *localDirectory) OpenReadWrite(name string, creationMode CreationMode) (FileReadWriter, error) {
	return d.open(name, creationMode, os.O_RDWR)
}

func (d *localDirectory) OpenWrite(name string, creationMode CreationMode) (FileWriter, error) {
	return d.open(name, creationMode, os.O_WRONLY)
}

func (d *localDirectory) Link(oldName string, newDirectory Directory, newName string) error {
	if err := validateFilename(oldName); err != nil {
		return err
	}
	if err := validateFilename(newName); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)
	return newDirectory.Apply(localDirectoryLink{
		oldFD:   d.fd,
		oldName: oldName,
		newName: newName,
	})
}

func (d *localDirectory) lstat(name string) (FileType, deviceNumber, error) {
	defer runtime.KeepAlive(d)

	var stat unix.Stat_t
	if err := unix.Fstatat(d.fd, name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return FileTypeOther, 0, err
	}
	fileType := FileTypeOther
	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		fileType = FileTypeDirectory
	case syscall.S_IFLNK:
		fileType = FileTypeSymlink
	case syscall.S_IFREG:
		if stat.Mode&0111 != 0 {
			fileType = FileTypeExecutableFile
		} else {
			fileType = FileTypeRegularFile
		}
	}
	return fileType, stat.Dev, nil
}

func (d *localDirectory) Lstat(name string) (FileInfo, error) {
	if err := validateFilename(name); err != nil {
		return FileInfo{}, err
	}
	fileType, _, err := d.lstat(name)
	if err != nil {
		return FileInfo{}, err
	}
	return NewFileInfo(name, fileType), nil
}

func (d *localDirectory) Mkdir(name string, perm os.FileMode) error {
	if err := validateFilename(name); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)

	return unix.Mkdirat(d.fd, name, uint32(perm))
}

func (d *localDirectory) readdirnames() ([]string, error) {
	defer runtime.KeepAlive(d)

	// Obtain filenames in current directory.
	fd, err := unix.Openat(d.fd, ".", unix.O_DIRECTORY|unix.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), ".")
	names, err := f.Readdirnames(-1)
	f.Close()
	return names, err
}

func (d *localDirectory) ReadDir() ([]FileInfo, error) {
	names, err := d.readdirnames()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	// Obtain file info.
	list := make([]FileInfo, 0, len(names))
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

var workingDirectoryLock sync.Mutex

func (d *localDirectory) unmount(name string) error {
	defer runtime.KeepAlive(d)

	// POSIX systems provide no umountat() system call that permits
	// us to unmount by directory handle. Use fchdir() to switch to
	// the parent directory first. Pick up a global lock to prevent
	// races on the working directory.
	workingDirectoryLock.Lock()
	defer workingDirectoryLock.Unlock()

	if err := syscall.Fchdir(d.fd); err != nil {
		return err
	}
	return syscall.Unmount(name, 0)
}

func (d *localDirectory) removeAllChildren(parentDeviceNumber deviceNumber) error {
	defer runtime.KeepAlive(d)

	names, err := d.readdirnames()
	if err != nil {
		return err
	}
	for _, name := range names {
		fileType, childDeviceNumber, err := d.lstat(name)
		if err != nil {
			return err
		}

		// The directory entry is a mount point. Repeatedly call
		// unmount until the remaining directory is on the same
		// file system.
		for parentDeviceNumber != childDeviceNumber {
			if err := d.unmount(name); err != nil {
				return err
			}
			fileType, childDeviceNumber, err = d.lstat(name)
			if err != nil {
				return err
			}
		}

		if fileType == FileTypeDirectory {
			// A directory. Remove all children. Adjust permissions
			// to ensure we can delete directories with degenerate
			// permissions.
			// TODO(edsch): This could use AT_SYMLINK_NOFOLLOW.
			// Unfortunately, this is broken on Linux.
			// Details: https://github.com/golang/go/issues/20130
			unix.Fchmodat(d.fd, name, 0700, 0)
			subdirectory, err := d.enter(name)
			if err != nil {
				return err
			}
			err = subdirectory.removeAllChildren(childDeviceNumber)
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

func (d *localDirectory) RemoveAllChildren() error {
	defer runtime.KeepAlive(d)

	var stat unix.Stat_t
	if err := unix.Fstat(d.fd, &stat); err != nil {
		return err
	}
	return d.removeAllChildren(stat.Dev)
}

func (d *localDirectory) RemoveAll(name string) error {
	if err := validateFilename(name); err != nil {
		return err
	}
	defer runtime.KeepAlive(d)

	// TODO(edsch): Call chmod(700) to ensure directory can be accessed?
	if subdirectory, err := d.EnterDirectory(name); err == nil {
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

type localDirectoryLink struct {
	oldFD   int
	oldName string
	newName string
}

func (d *localDirectory) Apply(arg interface{}) error {
	switch a := arg.(type) {
	case localDirectoryLink:
		defer runtime.KeepAlive(d)
		return unix.Linkat(a.oldFD, a.oldName, d.fd, a.newName, 0)
	default:
		return syscall.EXDEV
	}
}
