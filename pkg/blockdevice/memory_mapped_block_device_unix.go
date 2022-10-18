//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package blockdevice

import (
	"io"
	"runtime/debug"
	"syscall"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type memoryMappedBlockDevice struct {
	fd   int
	data []byte
}

// newMemoryMappedBlockDevice creates a BlockDevice from a file
// descriptor referring either to a regular file or UNIX device node. To
// speed up reads, a memory map is used.
func newMemoryMappedBlockDevice(fd, sizeBytes int) (BlockDevice, error) {
	data, err := unix.Mmap(fd, 0, sizeBytes, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to memory map block device")
	}
	return &memoryMappedBlockDevice{
		fd:   fd,
		data: data,
	}, nil
}

func (bd *memoryMappedBlockDevice) ReadAt(p []byte, off int64) (n int, err error) {
	// Let read actions go through the memory map to prevent system
	// call overhead for commonly requested objects.
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off > int64(len(bd.data)) {
		return 0, io.EOF
	}

	// Install a page fault handler, so that I/O errors against the
	// memory map (e.g., due to disk failure) don't cause us to
	// crash.
	old := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(old)
		if recover() != nil {
			err = status.Error(codes.Internal, "Page fault occurred while reading from memory map")
		}
	}()

	n = copy(p, bd.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
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
