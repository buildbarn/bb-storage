// +build freebsd

package filesystem

import (
	"os"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// deviceNumber is the equivalent of POSIX dev_t.
type deviceNumber = uint64

func (d *localDirectory) Mknod(name path.Component, perm os.FileMode, dev int) error {
	// Though mknodat() exists on FreeBSD, device nodes created
	// outside of devfs are non-functional.
	return status.Error(codes.Unimplemented, "Creation of device nodes is not supported on FreeBSD")
}
