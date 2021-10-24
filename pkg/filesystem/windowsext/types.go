// +build windows

package windowsext

import (
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

const (
	SYMLINK_FLAG_RELATIVE = 1
)

type REPARSE_DATA_BUFFER struct {
	ReparseTag        uint32
	ReparseDataLength uint16
	Reserved          uint16
	DUMMYUNIONNAME    [14]byte
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
