//go:build windows
// +build windows

package windowsext

// Several of the definitions in this file come from
// https://go.dev/src/internal/syscall/windows/reparse_windows.go.

import (
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

type FILE_ATTRIBUTE_TAG_INFO struct {
	FileAttributes uint32
	ReparseTag     uint32
}

type FILE_FULL_DIR_INFO struct {
	NextEntryOffset uint32
	FileIndex       uint32
	CreationTime    int64
	LastAccessTime  int64
	LastWriteTime   int64
	ChangeTime      int64
	EndOfFile       int64
	AllocationSize  int64
	FileAttributes  uint32
	FileNameLength  uint32
	EaSize          uint32
	FileName        [1]uint16
}

type FILE_ALLOCATED_RANGE_BUFFER struct {
	FileOffset int64
	Length     int64
}

type SymbolicLinkReparseBuffer struct {
	SubstituteNameOffset uint16
	SubstituteNameLength uint16
	PrintNameOffset      uint16
	PrintNameLength      uint16
	Flags                uint32
	PathBuffer           [1]uint16
}

func (rb *SymbolicLinkReparseBuffer) Path() string {
	n1 := rb.SubstituteNameOffset / 2
	n2 := (rb.SubstituteNameOffset + rb.SubstituteNameLength) / 2
	return syscall.UTF16ToString((*[0xffff]uint16)(unsafe.Pointer(&rb.PathBuffer[0]))[n1:n2:n2])
}

const (
	SYMLINK_FLAG_RELATIVE = 1
)

type REPARSE_DATA_BUFFER struct {
	ReparseTag        uint32
	ReparseDataLength uint16
	Reserved          uint16
	DUMMYUNIONNAME    byte
}

type REPARSE_DATA_BUFFER_HEADER struct {
	ReparseTag        uint32
	ReparseDataLength uint16
	Reserved          uint16
}

type FILE_DISPOSITION_INFORMATION_EX struct {
	Flags uint32
}

type FILE_LINK_INFORMATION struct {
	ReplaceIfExists uint8
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}

type FILE_RENAME_INFORMATION struct {
	ReplaceIfExists uint32
	RootDirectory   windows.Handle
	FileNameLength  uint32
	FileName        [1]uint16
}
