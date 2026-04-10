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

type fdBlockDevice struct {
	fd             int
	sizeBytes      int64
	data           []byte // non-nil when mmap is enabled
	syncAfterWrite bool
}

// newFDBlockDevice creates a BlockDevice from a file descriptor
// referring either to a regular file or UNIX device node. When useMmap
// is true, a read-only memory map is created to speed up reads.
// Otherwise, pread() is used for reads.
func newFDBlockDevice(fd, sizeBytes int, useMmap, syncAfterWrite bool) (BlockDevice, error) {
	bd := &fdBlockDevice{
		fd:             fd,
		sizeBytes:      int64(sizeBytes),
		syncAfterWrite: syncAfterWrite,
	}
	if useMmap {
		data, err := unix.Mmap(fd, 0, sizeBytes, syscall.PROT_READ, syscall.MAP_SHARED)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to memory map block device")
		}
		bd.data = data
	}
	return bd, nil
}

func (bd *fdBlockDevice) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off >= bd.sizeBytes {
		return 0, io.EOF
	}

	if bd.data != nil {
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
		return n, err
	}

	nTotal := 0
	for len(p) > 0 {
		n, err := unix.Pread(bd.fd, p, off)
		nTotal += n
		if err != nil {
			return nTotal, err
		}
		if n == 0 {
			return nTotal, io.EOF
		}
		p = p[n:]
		off += int64(n)
	}
	return nTotal, nil
}

func (bd *fdBlockDevice) WriteAt(p []byte, off int64) (int, error) {
	// Let write actions go through the file descriptor. Doing so
	// yields better performance, as writes through a memory map
	// would trigger a page fault that causes data to be read.
	//
	// The pwrite() system call cannot return a size and error at
	// the same time. If an error occurs after one or more bytes are
	// written, it returns the size without an error (a "short
	// write"). As WriteAt() must return an error in those cases, we
	// must invoke pwrite() repeatedly.
	//
	// TODO: Maybe it makes sense to let unaligned writes that would
	// trigger reads anyway to go through the memory map?

	startOff := off
	nTotal := 0
	for len(p) > 0 {
		n, err := unix.Pwrite(bd.fd, p, off)
		nTotal += n
		if err != nil {
			return nTotal, err
		}
		p = p[n:]
		off += int64(n)
	}
	if bd.syncAfterWrite {
		if err := syncDataRange(bd.fd, startOff, int64(nTotal)); err != nil {
			return nTotal, util.StatusWrap(err, "Failed to sync data range after write")
		}
	}
	return nTotal, nil
}

func (bd *fdBlockDevice) Sync() error {
	return unix.Fsync(bd.fd)
}

func (bd *fdBlockDevice) Close() error {
	var errors []error

	if bd.data != nil {
		if err := unix.Munmap(bd.data); err != nil {
			errors = append(errors, util.StatusWrap(err, "Failed to unmap memory region"))
		}
	}

	if err := unix.Close(bd.fd); err != nil {
		errors = append(errors, util.StatusWrap(err, "Failed to close file descriptor"))
	}

	if len(errors) == 0 {
		return nil
	}
	return util.StatusFromMultiple(errors)
}
