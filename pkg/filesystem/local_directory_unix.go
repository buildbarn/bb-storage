//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package filesystem

import (
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
)

type localDirectory struct {
	fd int
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

func (d *localDirectory) enter(name path.Component) (*localDirectory, error) {
	defer runtime.KeepAlive(d)

	fd, err := unix.Openat(d.fd, name.String(), unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_RDONLY, 0)
	if err != nil {
		if runtime.GOOS == "freebsd" && err == syscall.EMLINK {
			// FreeBSD erroneously returns EMLINK.
			return nil, syscall.ENOTDIR
		} else if runtime.GOOS == "linux" && err == syscall.ELOOP {
			// Linux 3.10 returns ELOOP, while Linux 4.15 returns ENOTDIR. Prefer the latter.
			return nil, syscall.ENOTDIR
		}
		return nil, err
	}
	return newLocalDirectoryFromFileDescriptor(fd)
}

func (d *localDirectory) EnterDirectory(name path.Component) (DirectoryCloser, error) {
	return d.enter(name)
}

func (d *localDirectory) Close() error {
	fd := d.fd
	d.fd = -1
	runtime.SetFinalizer(d, nil)
	return unix.Close(fd)
}

func (d *localDirectory) open(name path.Component, creationMode CreationMode, flag int) (*os.File, error) {
	defer runtime.KeepAlive(d)

	fd, err := unix.Openat(d.fd, name.String(), flag|creationMode.flags|unix.O_NOFOLLOW, uint32(creationMode.permissions))
	if err != nil {
		if runtime.GOOS == "freebsd" && err == syscall.EMLINK {
			// FreeBSD erroneously returns EMLINK.
			return nil, syscall.ELOOP
		}
		return nil, err
	}
	return os.NewFile(uintptr(fd), name.String()), nil
}

func (d *localDirectory) OpenAppend(name path.Component, creationMode CreationMode) (FileAppender, error) {
	return d.open(name, creationMode, os.O_APPEND|os.O_WRONLY)
}

type localFileReadWriter struct {
	*os.File
}

func (f localFileReadWriter) GetNextRegionOffset(offset int64, regionType RegionType) (int64, error) {
	defer runtime.KeepAlive(f)

	var whence int
	switch regionType {
	case Data:
		whence = unix.SEEK_DATA
	case Hole:
		whence = unix.SEEK_HOLE
	default:
		panic("Unknown region type")
	}
	nextOffset, err := unix.Seek(int(f.Fd()), offset, whence)
	if err == syscall.ENXIO {
		return 0, io.EOF
	}
	return nextOffset, err
}

func (d *localDirectory) OpenRead(name path.Component) (FileReader, error) {
	f, err := d.open(name, DontCreate, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	return localFileReadWriter{File: f}, nil
}

func (d *localDirectory) OpenReadWrite(name path.Component, creationMode CreationMode) (FileReadWriter, error) {
	f, err := d.open(name, creationMode, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	return localFileReadWriter{File: f}, nil
}

func (d *localDirectory) OpenWrite(name path.Component, creationMode CreationMode) (FileWriter, error) {
	f, err := d.open(name, creationMode, os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	return localFileReadWriter{File: f}, nil
}

func (d *localDirectory) Link(oldName path.Component, newDirectory Directory, newName path.Component) error {
	defer runtime.KeepAlive(d)
	return newDirectory.Apply(localDirectoryLink{
		oldFD:   d.fd,
		oldName: oldName,
		newName: newName,
	})
}

func (d *localDirectory) Clonefile(oldName path.Component, newDirectory Directory, newName path.Component) error {
	defer runtime.KeepAlive(d)
	return newDirectory.Apply(localDirectoryClonefile{
		oldFD:   d.fd,
		oldName: oldName,
		newName: newName,
	})
}

func (d *localDirectory) lstat(name path.Component) (FileType, rawDeviceNumber, bool, error) {
	defer runtime.KeepAlive(d)

	var stat unix.Stat_t
	if err := unix.Fstatat(d.fd, name.String(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return FileTypeOther, 0, false, err
	}
	fileType := FileTypeOther
	isExecutable := false
	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFDIR:
		fileType = FileTypeDirectory
	case syscall.S_IFLNK:
		fileType = FileTypeSymlink
	case syscall.S_IFREG:
		fileType = FileTypeRegularFile
		isExecutable = stat.Mode&0o111 != 0
	case syscall.S_IFBLK:
		fileType = FileTypeBlockDevice
	case syscall.S_IFCHR:
		fileType = FileTypeCharacterDevice
	case syscall.S_IFIFO:
		fileType = FileTypeFIFO
	case syscall.S_IFSOCK:
		fileType = FileTypeSocket
	}
	return fileType, stat.Dev, isExecutable, nil
}

func (d *localDirectory) Lstat(name path.Component) (FileInfo, error) {
	fileType, _, isExecutable, err := d.lstat(name)
	if err != nil {
		return FileInfo{}, err
	}
	return NewFileInfo(name, fileType, isExecutable), nil
}

func (d *localDirectory) Mkdir(name path.Component, perm os.FileMode) error {
	defer runtime.KeepAlive(d)

	return unix.Mkdirat(d.fd, name.String(), uint32(perm))
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
		info, err := d.Lstat(path.MustNewComponent(name))
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

func (d *localDirectory) Readlink(name path.Component) (string, error) {
	defer runtime.KeepAlive(d)

	for l := 128; ; l *= 2 {
		b := make([]byte, l)
		n, err := unix.Readlinkat(d.fd, name.String(), b)
		if err != nil {
			return "", err
		}
		if n < l {
			return string(b[0:n]), nil
		}
	}
}

func (d *localDirectory) Remove(name path.Component) error {
	defer runtime.KeepAlive(d)

	// First try deleting it as a regular file.
	err1 := unix.Unlinkat(d.fd, name.String(), 0)
	if err1 == nil {
		return nil
	}
	// Then try to delete it as a directory.
	err2 := unix.Unlinkat(d.fd, name.String(), unix.AT_REMOVEDIR)
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

func (d *localDirectory) Unmount(name path.Component) error {
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
	return syscall.Unmount(name.String(), 0)
}

func (d *localDirectory) removeAllChildren(parentDeviceNumber rawDeviceNumber) error {
	defer runtime.KeepAlive(d)

	names, err := d.readdirnames()
	if err != nil {
		return err
	}
	for _, name := range names {
		component := path.MustNewComponent(name)
		fileType, childDeviceNumber, _, err := d.lstat(component)
		if err != nil {
			return err
		}

		// The directory entry is a mount point. Repeatedly call
		// unmount until the remaining directory is on the same
		// file system.
		for parentDeviceNumber != childDeviceNumber {
			if err := d.Unmount(component); err != nil {
				return err
			}
			fileType, childDeviceNumber, _, err = d.lstat(component)
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
			unix.Fchmodat(d.fd, name, 0o700, 0)
			subdirectory, err := d.enter(component)
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

func (d *localDirectory) RemoveAll(name path.Component) error {
	defer runtime.KeepAlive(d)

	// TODO(edsch): Call chmod(700) to ensure directory can be accessed?
	if subdirectory, err := d.EnterDirectory(name); err == nil {
		// A directory. Remove all children.
		err := subdirectory.RemoveAllChildren()
		subdirectory.Close()
		if err != nil {
			return err
		}
		return unix.Unlinkat(d.fd, name.String(), unix.AT_REMOVEDIR)
	} else if err == syscall.ENOTDIR {
		// Not a directory. Remove it immediately.
		return unix.Unlinkat(d.fd, name.String(), 0)
	} else {
		return err
	}
}

func (d *localDirectory) Rename(oldName path.Component, newDirectory Directory, newName path.Component) error {
	defer runtime.KeepAlive(d)
	return newDirectory.Apply(localDirectoryRename{
		oldFD:   d.fd,
		oldName: oldName,
		newName: newName,
	})
}

func (d *localDirectory) Symlink(oldName string, newName path.Component) error {
	defer runtime.KeepAlive(d)

	return unix.Symlinkat(oldName, d.fd, newName.String())
}

func (d *localDirectory) Sync() error {
	defer runtime.KeepAlive(d)

	return unix.Fsync(d.fd)
}

func (d *localDirectory) Chtimes(name path.Component, atime, mtime time.Time) error {
	defer runtime.KeepAlive(d)

	var ts [2]unix.Timespec
	var err error
	if ts[0], err = unix.TimeToTimespec(atime); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot convert access time")
	}
	if ts[1], err = unix.TimeToTimespec(mtime); err != nil {
		return util.StatusWrapWithCode(err, codes.InvalidArgument, "Cannot convert modification time")
	}

	return unix.UtimesNanoAt(d.fd, name.String(), ts[:], unix.AT_SYMLINK_NOFOLLOW)
}

func (d *localDirectory) IsWritable() (bool, error) {
	return d.isWritable(".")
}

func (d *localDirectory) IsWritableChild(name path.Component) (bool, error) {
	return d.isWritable(name.String())
}

func (d *localDirectory) isWritable(name string) (bool, error) {
	defer runtime.KeepAlive(d)
	err := unix.Faccessat(d.fd, name, unix.W_OK, unix.AT_SYMLINK_NOFOLLOW)
	if os.IsPermission(err) || err == syscall.EROFS {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

type localDirectoryLink struct {
	oldFD   int
	oldName path.Component
	newName path.Component
}

type localDirectoryRename struct {
	oldFD   int
	oldName path.Component
	newName path.Component
}

type localDirectoryClonefile struct {
	oldFD   int
	oldName path.Component
	newName path.Component
}

func (d *localDirectory) Apply(arg interface{}) error {
	switch a := arg.(type) {
	case localDirectoryLink:
		defer runtime.KeepAlive(d)
		return unix.Linkat(a.oldFD, a.oldName.String(), d.fd, a.newName.String(), 0)
	case localDirectoryRename:
		defer runtime.KeepAlive(d)
		return unix.Renameat(a.oldFD, a.oldName.String(), d.fd, a.newName.String())
	case localDirectoryClonefile:
		defer runtime.KeepAlive(d)
		return clonefileImpl(a.oldFD, a.oldName.String(), d.fd, a.newName.String())
	default:
		return syscall.EXDEV
	}
}
