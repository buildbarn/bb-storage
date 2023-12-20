//go:build linux
// +build linux

package filesystem

import (
	"os"
	"runtime"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rawDeviceNumber is the equivalent of POSIX dev_t.
type rawDeviceNumber = uint64

func (d *localDirectory) Mknod(name path.Component, perm os.FileMode, deviceNumber DeviceNumber) error {
	defer runtime.KeepAlive(d)

	var unixPerm uint32
	switch perm & os.ModeType {
	case os.ModeDevice | os.ModeCharDevice:
		unixPerm = uint32(unix.S_IFCHR | (perm & os.ModePerm))
	default:
		return status.Error(codes.InvalidArgument, "The provided file mode is not for a character device")
	}

	return unix.Mknodat(d.fd, name.String(), unixPerm, int(deviceNumber.ToRaw()))
}

func clonefileImpl(oldFD int, oldName string, newFD int, newName string) error {
	return status.Error(codes.Unimplemented, "Clonefile is only supported on Darwin")
}

func (d *localDirectory) Mount(mountpoint path.Component, source, fstype string) error {
	mountname := mountpoint.String()
	fd, err := unix.Fsopen(fstype, unix.FSOPEN_CLOEXEC)
	if err != nil {
		return util.StatusWrapf(err, "Fsopen '%s'", fstype)
	}
	defer unix.Close(fd)

	err = unix.FsconfigSetString(fd, "source", source)
	if err != nil {
		return util.StatusWrapf(err, "Fsconfig source '%s'", source)
	}

	err = unix.FsconfigCreate(fd)
	if err != nil {
		return util.StatusWrap(err, "Fsconfig create")
	}

	mfd, err := unix.Fsmount(fd, unix.FSMOUNT_CLOEXEC, unix.MS_NOEXEC)
	if err != nil {
		return util.StatusWrap(err, "Fsmount")
	}
	// NB: `Fsmount` creates a file descriptor to the mount object, that can be
	// used to move it again. But we will not do so, so it is best to close it.
	// Unmount will fail with `EBUSY` if it is left open.
	defer unix.Close(mfd)

	err = unix.MoveMount(mfd, "", d.fd, mountname, unix.MOVE_MOUNT_F_EMPTY_PATH)
	if err != nil {
		return util.StatusWrapf(err, "Movemount mountname '%s' in file descriptor %d", mountname, d.fd)
	}

	return nil
}
