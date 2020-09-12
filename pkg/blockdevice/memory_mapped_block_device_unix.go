// +build darwin freebsd linux

package blockdevice

import (
	"io"
	"syscall"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
)

type memoryMappedBlockDevice struct {
	fd   int
	data []byte
}

// newMemoryMappedBlockDevice creates a BlockDevice from a file
// descriptor referring either to a regular file or UNIX device node. To
// speed up reads, a memory map is used.
func newMemoryMappedBlockDevice(fd int, sizeBytes int) (BlockDevice, error) {
	data, err := unix.Mmap(fd, 0, sizeBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to memory map block device")
	}
	return &memoryMappedBlockDevice{
		fd:   fd,
		data: data,
	}, nil
}

func (bd *memoryMappedBlockDevice) ReadAt(p []byte, off int64) (int, error) {
	// Let read actions go through the memory map to prevent system
	// call overhead for commonly requested objects.
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off > int64(len(bd.data)) {
		return 0, io.EOF
	}
	n := copy(p, bd.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (bd *memoryMappedBlockDevice) WriteAt(p []byte, off int64) (int, error) {
	// Let write actions go through the file descriptor. Doing so
	// yields better performance, as writes through a memory map
	// would trigger a page fault that causes data to be read.
	//
	// TODO: Maybe it makes sense to let unaligned writes that would
	// trigger reads anyway to go through the memory map?
	return unix.Pwrite(bd.fd, p, off)
}

func (bd *memoryMappedBlockDevice) Sync() error {
	return unix.Fsync(bd.fd)
}
