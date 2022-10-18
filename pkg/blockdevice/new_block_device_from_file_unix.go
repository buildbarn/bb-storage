//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package blockdevice

import (
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

// NewBlockDeviceFromFile creates a BlockDevice that is backed by a
// regular file stored in a file system.
//
// This approach tends to have more overhead than BlockDevices created
// using NewBlockDeviceFromDevice, but is often easier to set up in
// environments where spare disks (or the privileges needed to access
// those) aren't readily available.
func NewBlockDeviceFromFile(path string, minimumSizeBytes int, zeroInitialize bool) (BlockDevice, int, int64, error) {
	flags := unix.O_CREAT | unix.O_RDWR
	if zeroInitialize {
		flags |= unix.O_TRUNC
	}
	fd, err := unix.Open(path, flags, 0o666)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to open file %#v", path)
	}

	// Use the block size returned by fstat() to determine the
	// sector size and the number of sectors needed to store the
	// desired amount of space.
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to obtain size of file %#v", path)
	}
	sectorSizeBytes := int(stat.Blksize)
	sectorCount := int64((uint64(minimumSizeBytes) + uint64(stat.Blksize) - 1) / uint64(stat.Blksize))
	sizeBytes := int64(sectorSizeBytes) * sectorCount

	if err := unix.Ftruncate(fd, sizeBytes); err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to truncate file %#v to %d bytes", path, sizeBytes)
	}

	bd, err := newMemoryMappedBlockDevice(fd, int(sizeBytes))
	if err != nil {
		unix.Close(fd)
		return nil, 0, 0, err
	}
	return bd, sectorSizeBytes, sectorCount, nil
}
