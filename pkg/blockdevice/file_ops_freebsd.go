package blockdevice

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

func allocateFile(fd int, sizeBytes int64) error {
	return unix.PosixFallocate(fd, 0, sizeBytes)
}

func syncDataRange(fd int, off, nbytes int64) error {
	return nil
}

func getBlockDeviceInfo(fd int) (sectorSizeBytes int, deviceSizeBytes int64, err error) {
	var sectorSize int32
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.DIOCGSECTORSIZE, uintptr(unsafe.Pointer(&sectorSize))); errno != 0 {
		return 0, 0, errno
	}
	var deviceSize int64
	if _, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.DIOCGMEDIASIZE, uintptr(unsafe.Pointer(&deviceSize))); errno != 0 {
		return 0, 0, errno
	}
	return int(sectorSize), deviceSize, nil
}
