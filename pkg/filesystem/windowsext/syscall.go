// +build windows

package windowsext

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	FILE_READ_ATTRIBUTES  = 0x80
	FILE_READ_DATA        = 1
	FILE_READ_EA          = 8
	FILE_WRITE_ATTRIBUTES = 0x100
	FILE_WRITE_DATA       = 2
	FILE_WRITE_EA         = 0x10
	FILE_GENERIC_READ     = windows.STANDARD_RIGHTS_READ | windows.SYNCHRONIZE | FILE_READ_ATTRIBUTES | FILE_READ_DATA | FILE_READ_EA
	FILE_GENERIC_WRITE    = windows.STANDARD_RIGHTS_WRITE | windows.SYNCHRONIZE | FILE_WRITE_ATTRIBUTES | FILE_WRITE_DATA | FILE_WRITE_EA

	FSCTL_SET_REPARSE_POINT      = 0x900A4
	FSCTL_QUERY_ALLOCATED_RANGES = 0x940CF
	FSCTL_SET_SPARSE             = 0x900C4

	FileRenameInformation        = 10
	FileLinkInformation          = 11
	FileDispositionInformationEx = 64
)

var (
	modntdll = windows.NewLazySystemDLL("ntdll.dll")

	procNtSetInformationFile = modntdll.NewProc("NtSetInformationFile")
)

func NtSetInformationFile(handle windows.Handle, iosb *windows.IO_STATUS_BLOCK, fileInfo *byte, len, class uint32) error {
	r0, _, _ := syscall.Syscall6(procNtSetInformationFile.Addr(), 5, uintptr(handle),
		uintptr(unsafe.Pointer(iosb)), uintptr(unsafe.Pointer(fileInfo)), uintptr(len), uintptr(class), 0)
	if r0 != 0 {
		return windows.NTStatus(r0)
	}
	return nil
}
