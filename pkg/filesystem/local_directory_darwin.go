// +build darwin

package filesystem

import (
	"os"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deviceNumber is the equivalent of POSIX dev_t.
type deviceNumber = int32

func (d *localDirectory) Mknod(name path.Component, perm os.FileMode, dev int) error {
	return status.Error(codes.Unimplemented, "Creation of device nodes is not supported on Darwin")
}

func clonefileImpl(oldFD int, oldName string, newFD int, newName string) error {
	return unix.Clonefileat(oldFD, oldName, newFD, newName, unix.CLONE_NOFOLLOW)
}
