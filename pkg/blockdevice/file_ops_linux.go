package blockdevice

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

func allocateFile(fd int, sizeBytes int64) error {
	return unix.Fallocate(fd, 0, 0, sizeBytes)
}

func syncDataRange(fd int, off, nbytes int64) error {
	return unix.SyncFileRange(fd, off, nbytes, unix.SYNC_FILE_RANGE_WRITE)
}

func getBlockDeviceInfo(fd int) (sectorSizeBytes int, deviceSizeBytes int64, err error) {
	var sectorSize int32
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKBSZGET, uintptr(unsafe.Pointer(&sectorSize))); errno != 0 {
		return 0, 0, errno
	}
	var deviceSize int64
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&deviceSize))); errno != 0 {
		return 0, 0, errno
	}
	return int(sectorSize), deviceSize, nil
}
