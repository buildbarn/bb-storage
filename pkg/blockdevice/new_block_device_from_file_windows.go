//go:build windows
// +build windows

package blockdevice

import (
	"fmt"
	"path/filepath"
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
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to get absolute path for %#v", path)
	}
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

	bd, sectorSizeBytes, sectorCount, err := createBlockDevice(path, minimumSizeBytes, zeroInitialize, handle)
	if err != nil {
		windows.CloseHandle(handle)
		return nil, 0, 0, err
	}
	return bd, sectorSizeBytes, sectorCount, nil
}

func createBlockDevice(path string, minimumSizeBytes int, zeroInitialize bool, handle windows.Handle) (BlockDevice, int, int64, error) {
	// Get the sector size from the disk where the file is located.

	// Extract the root path (drive letter) from the full path.
	rootPath, err := RootPathForPath(path)
	if err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to get root path for %#v", path)
	}
	rootPathPtr, err := syscall.UTF16PtrFromString(rootPath)
	if err != nil {
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
		return nil, 0, 0, util.StatusWrapf(err, "Failed to get disk sector size for %#v", rootPath)
	}

	sectorSizeBytes := int(bytesPerSector)

	// Calculate the number of sectors needed.
	sectorCount := int64((uint64(minimumSizeBytes) + uint64(sectorSizeBytes) - 1) / uint64(sectorSizeBytes))
	sizeBytes := int64(sectorSizeBytes) * sectorCount

	// Determine if we need to make the file sparse.
	// If we want to zero initialise the file, then we always make the
	// file sparse as this is the most efficient way of doing so.
	// We also mark the file as sparse if we need to resize the file.
	// If we do not mark the file as sparse, then when this process
	// terminates, Windows will write out the entire file's contents
	// to disk, even if only 1 byte has been written to the file. See
	// https://devblogs.microsoft.com/oldnewthing/20110922-00/?p=9573
	// for further details.
	var makeFileSparse bool
	if zeroInitialize {
		makeFileSparse = true
	} else {
		// Check if we need to resize the file.
		var fileInfo windows.ByHandleFileInformation
		if err := windows.GetFileInformationByHandle(handle, &fileInfo); err != nil {
			return nil, 0, 0, util.StatusWrapf(err, "Failed to get file information for %#v", path)
		}
		existingSize := int64(fileInfo.FileSizeHigh)<<32 | int64(fileInfo.FileSizeLow)
		makeFileSparse = existingSize < int64(minimumSizeBytes)
	}

	var fileSizeHigh int32 = int32(sizeBytes >> 32)
	if _, err = windows.SetFilePointer(handle, int32(sizeBytes), (*int32)(unsafe.Pointer(&fileSizeHigh)), windows.FILE_BEGIN); err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to set file pointer for %#v", path)
	}

	if err := windows.SetEndOfFile(handle); err != nil {
		return nil, 0, 0, util.StatusWrapf(err, "Failed to set end of file for %#v to %d bytes", path, sizeBytes)
	}

	if makeFileSparse {
		if err := makeSparseFile(handle, sizeBytes); err != nil {
			return nil, 0, 0, util.StatusWrapf(err, "Failed to make file sparse for %#v", path)
		}
	}

	bd, err := newMemoryMappedBlockDevice(handle, int(sizeBytes))
	if err != nil {
		return nil, 0, 0, err
	}
	return bd, sectorSizeBytes, sectorCount, nil
}

func makeSparseFile(handle windows.Handle, sizeBytes int64) error {
	// FILE_ZERO_DATA_INFORMATION
	type fileZeroDataInformation struct {
		FileOffset      int64
		BeyondFinalZero int64
	}

	// Mark the file as sparse.
	var bytesReturned uint32
	if err := windows.DeviceIoControl(
		handle,
		windows.FSCTL_SET_SPARSE,
		/*inBuffer=*/ nil,
		/*inBufferSize=*/ 0,
		/*outBuffer=*/ nil,
		/*outBufferSize=*/ 0,
		&bytesReturned,
		/*overlapped=*/ nil,
	); err != nil {
		return util.StatusWrapf(err, "Failed to mark file as sparse")
	}
	// Zero the file's data.
	zeroData := fileZeroDataInformation{
		FileOffset:      0,
		BeyondFinalZero: sizeBytes,
	}
	if err := windows.DeviceIoControl(
		handle,
		windows.FSCTL_SET_ZERO_DATA,
		(*byte)(unsafe.Pointer(&zeroData)),
		uint32(unsafe.Sizeof(zeroData)),
		/*outBuffer=*/ nil,
		/*outBufferSize=*/ 0,
		&bytesReturned,
		/*overlapped=*/ nil,
	); err != nil {
		return util.StatusWrapf(err, "Failed to zero sparse file data")
	}

	return nil
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
