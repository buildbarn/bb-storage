//go:build linux || freebsd

package blockdevice

import (
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
func NewBlockDeviceFromDevice(path string, useMmap, syncAfterWrite bool) (BlockDevice, int, int64, error) {
	fd, err := unix.Open(path, unix.O_RDWR, 0)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to open device node %#v", path)
	}

	sectorSizeBytes, deviceSizeBytes, err := getBlockDeviceInfo(fd)
	if err != nil {
		unix.Close(fd)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain device geometry for %#v", path)
	}

	bd, err := newFDBlockDevice(fd, int(deviceSizeBytes), useMmap, syncAfterWrite)
	if err != nil {
		unix.Close(fd)
		return nil, 0, 0, err
	}
	return bd, sectorSizeBytes, deviceSizeBytes / int64(sectorSizeBytes), nil
}
