//go:build windows
// +build windows

package blockdevice

import (
	"fmt"
	"syscall"
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"golang.org/x/sys/windows"
)

var (
	kernel32          = windows.NewLazySystemDLL("kernel32.dll")
	getDiskFreeSpaceW = kernel32.NewProc("GetDiskFreeSpaceW")
)

// NewBlockDeviceFromFile creates a BlockDevice that is backed by a
// regular file stored in a file system.
func NewBlockDeviceFromFile(path string, minimumSizeBytes int, zeroInitialize bool) (BlockDevice, int, int64, error) {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to convert path %#v to UTF-16", path)
	}

	var createmode uint32 = windows.OPEN_ALWAYS
	if zeroInitialize {
		createmode = windows.CREATE_ALWAYS
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		createmode,
		windows.FILE_ATTRIBUTE_NORMAL,
		0)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to open file %#v", path)
	}

	// Get the sector size from the disk where the file is located.

	// Extract the root path (drive letter) from the full path.
	rootPath, err := RootPathForPath(path)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to get root path for %#v", path)
	}
	rootPathPtr, err := syscall.UTF16PtrFromString(rootPath)
	if err != nil {
		windows.CloseHandle(handle)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to convert root path %#v to UTF-16", rootPath)
	}

	// Get the sector size using GetDiskFreeSpace via syscall since it's not in windows package.
	var bytesPerSector uint32
	r, _, err := getDiskFreeSpaceW.Call(
		uintptr(unsafe.Pointer(rootPathPtr)),
		/*sectorsPerCluster=*/ 0,
		uintptr(unsafe.Pointer(&bytesPerSector)),
		/*numberOfFreeClusters=*/ 0,
		/*totalNumberOfClusters=*/ 0)
	if r == 0 {
		windows.CloseHandle(handle)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to get disk sector size for %#v", rootPath)
	}

	sectorSizeBytes := int(bytesPerSector)

	// Calculate the number of sectors needed.
	sectorCount := int64((uint64(minimumSizeBytes) + uint64(sectorSizeBytes) - 1) / uint64(sectorSizeBytes))
	sizeBytes := int64(sectorSizeBytes) * sectorCount

	var fileSizeHigh int32 = int32(sizeBytes >> 32)
	if _, err = windows.SetFilePointer(handle, int32(sizeBytes), (*int32)(unsafe.Pointer(&fileSizeHigh)), windows.FILE_BEGIN); err != nil {
		windows.CloseHandle(handle)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to set file pointer for %#v", path)
	}

	if err := windows.SetEndOfFile(handle); err != nil {
		windows.CloseHandle(handle)
		return nil, 0, 0, util.StatusWrapf(err, "Failed to set end of file for %#v to %d bytes", path, sizeBytes)
	}

	bd, err := newMemoryMappedBlockDevice(handle, int(sizeBytes))
	if err != nil {
		windows.CloseHandle(handle)
		return nil, 0, 0, err
	}
	return bd, sectorSizeBytes, sectorCount, nil
}

// A ScopeWalker that extracts the root path for GetDiskFreeSpaceW.
type rootPathScopeWalker struct {
	rootPath string
	err      error
}

func (w *rootPathScopeWalker) OnAbsolute() (path.ComponentWalker, error) {
	w.err = status.Error(codes.InvalidArgument, "Path is absolute, while a path begining with a drive letter or a UNC path was expected")
	return path.VoidComponentWalker, nil
}

func (w *rootPathScopeWalker) OnDriveLetter(drive rune) (path.ComponentWalker, error) {
	// We want to return C:\, including the trailing backslash
	// (see GetDiskFreeSpaceW).
	w.rootPath = fmt.Sprintf(`%c:\`, drive)
	return path.VoidComponentWalker, nil
}

func (w *rootPathScopeWalker) OnRelative() (path.ComponentWalker, error) {
	w.err = status.Error(codes.InvalidArgument, "Path is relative, while a path begining with a drive letter or a UNC path was expected")
	return path.VoidComponentWalker, nil
}

func (w *rootPathScopeWalker) OnShare(server, share string) (path.ComponentWalker, error) {
	// We want to return \\server\share\, including the trailing backslash
	// (see GetDiskFreeSpaceW).
	w.rootPath = fmt.Sprintf(`\\%s\%s\`, server, share)
	return path.VoidComponentWalker, nil
}

// Computes the root path for a given windows file path as required for GetDiskFreeSpaceW.
func RootPathForPath(in string) (string, error) {
	w := rootPathScopeWalker{}
	if err := path.Resolve(path.LocalFormat.NewParser(in), &w); err != nil {
		return "", err
	}
	if w.err != nil {
		return "", w.err
	}
	return w.rootPath, nil
}
