// Inspired by
// http://www.flexhex.com/docs/articles/hard-links.phtml
// https://golang.org/src/os/os_windows_test.go
// https://gist.github.com/Perlmint/f9f0e37db163dd69317d

// +build windows

package filesystem

import (
	"internal/syscall/windows"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

type reparseDataBuffer struct {
	header windows.REPARSE_DATA_BUFFER_HEADER
	data   [syscall.MAXIMUM_REPARSE_DATA_BUFFER_SIZE]byte
}

const (
	// The size of .PathBuffer when reparseDataBufferdata is reinterpreted to windows.MountPointReparseBuffer
	pathBufferByteSize = syscall.MAXIMUM_REPARSE_DATA_BUFFER_SIZE - unsafe.Offsetof(windows.MountPointReparseBuffer{}.PathBuffer)
)

func newJunctionReparseDataBuffer(targetAbsolutePath string) (reparseDataBuffer, uint32) {
	var junctionInfo reparseDataBuffer
	junctionData := (*windows.MountPointReparseBuffer)(unsafe.Pointer(&junctionInfo.data[0]))

	if !strings.HasPrefix(targetAbsolutePath, `\\?\`) {
		targetAbsolutePath = `\\?\` + targetAbsolutePath
	}
	substituteName := syscall.StringToUTF16(targetAbsolutePath)
	// Skip printName to leave more buffer space for potential long targetAbsolutePath
	printName := syscall.StringToUTF16("") // targetAbsolutePath

	// Skip the terminating NUL in the length, but copy it to the buffer
	var pathBuf []uint16
	junctionData.SubstituteNameOffset = uint16(len(pathBuf) * 2)
	junctionData.SubstituteNameLength = uint16((len(substituteName) - 1) * 2) // Exclude the NUL terminator
	pathBuf = append(pathBuf, substituteName...)                              // Include the NUL terminator
	junctionData.PrintNameOffset = uint16(len(pathBuf) * 2)
	junctionData.PrintNameLength = uint16((len(printName) - 1) * 2) // Exclude the NUL terminator
	pathBuf = append(pathBuf, printName...)                         // Include the NUL terminator
	copy((*[pathBufferByteSize / 2]uint16)(unsafe.Pointer(&junctionData.PathBuffer[0]))[:], pathBuf)

	junctionInfo.header.ReparseTag = windows.IO_REPARSE_TAG_MOUNT_POINT
	junctionInfo.header.ReparseDataLength = uint16(unsafe.Offsetof(junctionData.PathBuffer)) + uint16(len(pathBuf)*2)
	junctionInfoLen := uint32(unsafe.Offsetof(junctionInfo.data)) + uint32(junctionInfo.header.ReparseDataLength)
	return junctionInfo, junctionInfoLen
}

func MakeDirectoryJunction(link, targetAbsolutePath string) error {
	junctionInfo, junctionInfoLen := newJunctionReparseDataBuffer(targetAbsolutePath)

	err := os.Mkdir(link, 0777)
	if err != nil {
		return err
	}
	linkPtr := syscall.StringToUTF16Ptr(link)
	hlink, err := syscall.CreateFile(linkPtr, syscall.GENERIC_WRITE,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE, nil, syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_OPEN_REPARSE_POINT|syscall.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(hlink)

	var bytesReturned uint32
	err = syscall.DeviceIoControl(hlink, windows.FSCTL_SET_REPARSE_POINT,
		(*byte)(unsafe.Pointer(&junctionInfo)), junctionInfoLen, nil, 0, &bytesReturned, nil)
	if err != nil {
		return err
	}
	return err
}
