// +build linux

package filesystem

import (
	"os"
	"runtime"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deviceNumber is the equivalent of POSIX dev_t.
type deviceNumber = uint64

func (d *localDirectory) Mknod(name path.Component, perm os.FileMode, dev int) error {
	defer runtime.KeepAlive(d)

	var unixPerm uint32
	switch perm & os.ModeType {
	case os.ModeDevice | os.ModeCharDevice:
		unixPerm = uint32(unix.S_IFCHR | (perm & os.ModePerm))
	default:
		return status.Error(codes.InvalidArgument, "The provided file mode is not for a character device")
	}

	return unix.Mknodat(d.fd, name.String(), unixPerm, dev)
}

func clonefileImpl(oldFd int, oldName string, newFd int, newName string) error {
	return status.Error(codes.Unimplemented, "Clonefile is only supported on Darwin")
}
