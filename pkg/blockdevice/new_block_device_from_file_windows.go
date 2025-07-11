//go:build windows
// +build windows

package blockdevice

import (
	"strings"
	"syscall"
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/util"

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
	rootPath := RootPathForPath(path)
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

// Computes the root path for a given windows file path as required for GetDiskFreeSpaceW.
func RootPathForPath(path string) string {
	if len(path) >= 3 && path[1] == ':' && path[2] == '\\' {
		// Conventional disk-path; e.g. C:\somefolder.
		return path[:3]
	}
	if strings.HasPrefix(path, `\\`) {
		// UNC path; e.g. \\server\share\some_folder.
		// We want to return \\server\share\, including the trailing slash as per GetDiskFreeSpaceW's documentation.
		serverEnd := strings.IndexByte(path[2:], '\\')
		if serverEnd == -1 {
			return path
		}
		shareStart := 2 + serverEnd + 1
		shareEnd := strings.IndexByte(path[shareStart:], '\\')
		if shareEnd == -1 {
			return path
		}
		return path[:shareStart+shareEnd+1]
	}
	return path
}
