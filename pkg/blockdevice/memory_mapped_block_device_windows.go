//go:build windows
// +build windows

package blockdevice

import (
	"io"
	"runtime/debug"
	"syscall"
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type memoryMappedBlockDevice struct {
	fileHandle windows.Handle
	data       []byte
}

// newMemoryMappedBlockDevice creates a BlockDevice from a Windows file
// handle referring to a regular file.
func newMemoryMappedBlockDevice(fileHandle windows.Handle, sizeBytes int) (BlockDevice, error) {
	mapHandle, err := windows.CreateFileMapping(fileHandle, nil, windows.PAGE_READWRITE, uint32(sizeBytes>>32), uint32(sizeBytes), nil)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create file mapping for block device")
	}
	defer windows.CloseHandle(mapHandle)

	addrUIntPtr, err := windows.MapViewOfFile(mapHandle, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, 0, uintptr(sizeBytes))
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to map view of file for block device")
	}

	// MapViewOfFile can return smaller views than requested if the file is larger than the address space
	// supports (e.g., on 32-bit systems). Double-check this.
	var memInfo windows.MemoryBasicInformation
	if err := windows.VirtualQuery(addrUIntPtr, &memInfo, unsafe.Sizeof(memInfo)); err != nil {
		windows.UnmapViewOfFile(addrUIntPtr)
		return nil, util.StatusWrap(err, "Failed to query mapped view size for block device")
	}
	if memInfo.RegionSize < uintptr(sizeBytes) {
		windows.UnmapViewOfFile(addrUIntPtr)
		return nil, status.Errorf(codes.InvalidArgument, "Mapped view size is smaller than requested size: got %d bytes, expected at least %d bytes", memInfo.RegionSize, sizeBytes)
	}

	// Workaround go-vet unsafe ptr warning https://github.com/golang/go/issues/58625.
	addr := *(*unsafe.Pointer)(unsafe.Pointer(&addrUIntPtr))
	return &memoryMappedBlockDevice{
		fileHandle: fileHandle,
		data:       unsafe.Slice((*byte)(addr), sizeBytes),
	}, nil
}

func (bd *memoryMappedBlockDevice) ReadAt(p []byte, off int64) (n int, err error) {
	// Behave like the unix implementation.
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if off > int64(len(bd.data)) {
		return 0, io.EOF
	}

	old := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(old)
		if r := recover(); r != nil {
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
	// Like the unix implementation, we let write actions go through
	// the file handle.
	nTotal := 0
	for len(p) > 0 {
		var overlapped windows.Overlapped
		overlapped.Offset = uint32(off)
		overlapped.OffsetHigh = uint32(off >> 32)
		var bytesWritten uint32
		err := windows.WriteFile(bd.fileHandle, p, &bytesWritten, &overlapped)
		nTotal += int(bytesWritten)
		if err != nil {
			return nTotal, err
		}
		p = p[bytesWritten:]
		off += int64(bytesWritten)
	}
	return nTotal, nil
}

func (bd *memoryMappedBlockDevice) Sync() error {
	if err := windows.FlushViewOfFile(uintptr(unsafe.Pointer(&bd.data[0])), 0); err != nil {
		return util.StatusWrap(err, "Failed to flush view of file")
	}
	if err := windows.FlushFileBuffers(bd.fileHandle); err != nil {
		return util.StatusWrap(err, "Failed to flush file buffers")
	}
	return nil
}

func (bd *memoryMappedBlockDevice) Close() error {
	var errors []error

	if err := windows.UnmapViewOfFile(uintptr(unsafe.Pointer(&bd.data[0]))); err != nil {
		errors = append(errors, util.StatusWrap(err, "Failed to unmap view of file"))
	}

	if err := windows.CloseHandle(bd.fileHandle); err != nil {
		errors = append(errors, util.StatusWrap(err, "Failed to close file handle"))
	}

	if len(errors) > 0 {
		return util.StatusFromMultiple(errors)
	}
	return nil
}
