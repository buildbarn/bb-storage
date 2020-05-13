// +build linux

package blockdevice

import (
	"io"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type memoryMap struct {
	fd   int
	data []byte
}

// MemoryMapBlockDevice maps the entire contents of a block device into
// the address space of the current process. Access to the memory map is
// provided in the form of an io.ReaderAt/io.WriterAt.
//
// The sector size of the block device and the total number of sectors
// are also returned. It may be assumed that these remain constant over
// the lifetime of the block device and process.
//
// Writes may only occur at sector boundaries, as unaligned writes would
// cause unnecessary read operations against underlying storage.
func MemoryMapBlockDevice(path string) (ReadWriterAt, int, int64, error) {
	fd, err := unix.Open(path, unix.O_RDWR, 0)
	if err != nil {
		return nil, 0, 0, err
	}

	// Obtain the size of the device and its individual sectors.
	var sectorSizeBytes int32
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKBSZGET, uintptr(unsafe.Pointer(&sectorSizeBytes))); err != 0 {
		unix.Close(fd)
		return nil, 0, 0, err
	}
	var deviceSizeBytes int64
	if _, _, err := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), unix.BLKGETSIZE64, uintptr(unsafe.Pointer(&deviceSizeBytes))); err != 0 {
		unix.Close(fd)
		return nil, 0, 0, err
	}

	// Map the block device into the address space.
	data, err := unix.Mmap(fd, 0, int(deviceSizeBytes), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		unix.Close(fd)
		return nil, 0, 0, err
	}

	return &memoryMap{
		fd:   fd,
		data: data,
	}, int(sectorSizeBytes), deviceSizeBytes / int64(sectorSizeBytes), nil
}

func (mm *memoryMap) ReadAt(p []byte, off int64) (int, error) {
	// Let read actions go through the memory map to prevent system
	// call overhead for commonly requested objects.
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off > int64(len(mm.data)) {
		return 0, io.EOF
	}
	n := copy(p, mm.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (mm *memoryMap) WriteAt(p []byte, off int64) (int, error) {
	// Let write actions go through the file descriptor. Doing so
	// yields better performance, as writes through a memory map
	// would trigger a page fault that causes data to be read.
	//
	// TODO: Maybe it makes sense to let unaligned writes that would
	// trigger reads anyway to go through the memory map?
	return unix.Pwrite(mm.fd, p, off)
}
