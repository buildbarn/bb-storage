//go:build freebsd
// +build freebsd

package blockdevice

import (
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

// NewBlockDeviceFromDevice maps the entire contents of a block device
// into the address space of the current process. Access to the memory
// map is provided in the form of an io.ReaderAt/io.WriterAt.
//
// The sector size of the block device and the total number of sectors
// are also returned. It may be assumed that these remain constant over
// the lifetime of the block device and process.
//
// Writes may only occur at sector boundaries, as unaligned writes would
// cause unnecessary read operations against underlying storage.
func NewBlockDeviceFromDevice(path string) (BlockDevice, int, int64, error) {
	fd, err := unix.Open(path, unix.O_RDWR, 0)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to open device node %#v", path)
	}

	// Obtain the size of the device and its individual sectors.
	var sectorSizeBytes int32
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.DIOCGSECTORSIZE, uintptr(unsafe.Pointer(&sectorSizeBytes))); err != 0 {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain sector size of device node %#v", path)
	}
	var deviceSizeBytes int64
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.DIOCGMEDIASIZE, uintptr(unsafe.Pointer(&deviceSizeBytes))); err != 0 {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain media size of device node %#v", path)
	}

	bd, err := newMemoryMappedBlockDevice(fd, int(deviceSizeBytes))
	if err != nil {
		unix.Close(fd)
		return nil, 0, 0, err
	}
	return bd, int(sectorSizeBytes), deviceSizeBytes / int64(sectorSizeBytes), nil
}
