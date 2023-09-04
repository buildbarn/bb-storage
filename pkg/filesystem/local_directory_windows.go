//go:build windows
// +build windows

package filesystem

import (
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
	"github.com/buildbarn/bb-storage/pkg/filesystem/windowsext"

	"golang.org/x/sys/windows"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func ntCreateFile(handle *windows.Handle, access uint32, root windows.Handle, path string, disposition, options uint32) error {
	objectName, err := windows.NewNTUnicodeString(path)
	if err != nil {
		return err
	}
	oa := &windows.OBJECT_ATTRIBUTES{
		RootDirectory: root,
		ObjectName:    objectName,
	}
	oa.Length = uint32(unsafe.Sizeof(*oa))
	var iosb windows.IO_STATUS_BLOCK
	var allocSize int64 = 0
	ntstatus := windows.NtCreateFile(handle, access, oa, &iosb, &allocSize, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition, options, 0, 0)
	if ntstatus != nil {
		switch ntstatus.(windows.NTStatus) {
		case windows.STATUS_NOT_A_DIRECTORY:
			return syscall.ENOTDIR
		case windows.STATUS_FILE_IS_A_DIRECTORY:
			return syscall.EISDIR
		case windows.STATUS_OBJECT_NAME_EXISTS:
			return os.ErrExist
		default:
			return ntstatus.(windows.NTStatus).Errno()
		}
	}
	return nil
}

func isReparsePointByHandle(handle windows.Handle) (isReparsePoint bool, reparseTag uint32, err error) {
	var fileAttrTagInfo windowsext.FILE_ATTRIBUTE_TAG_INFO
	err = windows.GetFileInformationByHandleEx(handle, windows.FileAttributeTagInfo,
		(*byte)(unsafe.Pointer(&fileAttrTagInfo)), uint32(unsafe.Sizeof(fileAttrTagInfo)))
	if err != nil {
		return
	}
	isReparsePoint = (fileAttrTagInfo.FileAttributes & windows.FILE_ATTRIBUTE_REPARSE_POINT) == windows.FILE_ATTRIBUTE_REPARSE_POINT
	reparseTag = fileAttrTagInfo.ReparseTag
	return
}

type localDirectory struct {
	handle windows.Handle
}

func newLocalDirectoryFromHandle(handle windows.Handle) (*localDirectory, error) {
	d := &localDirectory{
		handle: handle,
	}
	runtime.SetFinalizer(d, (*localDirectory).Close)
	return d, nil
}

func newLocalDirectory(absPath string, openReparsePoint bool) (DirectoryCloser, error) {
	var handle windows.Handle
	var options uint32 = windows.FILE_DIRECTORY_FILE
	if openReparsePoint {
		options |= windows.FILE_OPEN_REPARSE_POINT
	}
	err := ntCreateFile(&handle, windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE|windows.GENERIC_WRITE,
		0, absPath, windows.FILE_OPEN, options)
	if err != nil {
		return nil, err
	}
	if openReparsePoint {
		isReparsePoint, reparsePointTag, err := isReparsePointByHandle(handle)
		if err != nil {
			windows.CloseHandle(handle)
			return nil, err
		}
		if isReparsePoint {
			windows.CloseHandle(handle)
			if reparsePointTag == windows.IO_REPARSE_TAG_SYMLINK {
				// Mimic the behavior of O_NOFOLLOW.
				return nil, syscall.ENOTDIR
			}
			// This is not a symlink (e.g. mount point). Reopen without the OPEN_REPARSE_POINT flag.
			// Cases where handle is a reparse point but not a symlink should be very rare.
			return newLocalDirectory(absPath, false)
		}
	}
	return newLocalDirectoryFromHandle(handle)
}

func NewLocalDirectory(path string) (DirectoryCloser, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	absPath = "\\??\\" + absPath
	return newLocalDirectory(absPath, true)
}

func (d *localDirectory) enter(name path.Component, openReparsePoint bool) (*localDirectory, error) {
	defer runtime.KeepAlive(d)

	var handle windows.Handle
	var options uint32 = windows.FILE_DIRECTORY_FILE
	if openReparsePoint {
		options |= windows.FILE_OPEN_REPARSE_POINT
	}
	err := ntCreateFile(&handle, windows.FILE_LIST_DIRECTORY|windows.FILE_TRAVERSE|windows.FILE_READ_ATTRIBUTES|windows.SYNCHRONIZE|windows.GENERIC_WRITE,
		d.handle, name.String(), windows.FILE_OPEN, options)
	if err != nil {
		return nil, err
	}
	if openReparsePoint {
		isReparsePoint, reparsePointTag, err := isReparsePointByHandle(handle)
		if err != nil {
			windows.CloseHandle(handle)
			return nil, err
		}
		if isReparsePoint {
			windows.CloseHandle(handle)
			if reparsePointTag == windows.IO_REPARSE_TAG_SYMLINK {
				return nil, syscall.ENOTDIR
			}
			return d.enter(name, false)
		}
	}
	return newLocalDirectoryFromHandle(handle)
}

func (d *localDirectory) EnterDirectory(name path.Component) (DirectoryCloser, error) {
	return d.enter(name, true)
}

func (d *localDirectory) Close() error {
	handle := d.handle
	d.handle = windows.InvalidHandle
	runtime.SetFinalizer(d, nil)
	return windows.CloseHandle(handle)
}

func (d *localDirectory) openNt(name path.Component, access, disposition uint32, openReparsePoint bool) (*os.File, error) {
	var handle windows.Handle
	var options uint32 = windows.FILE_NON_DIRECTORY_FILE | windows.FILE_SYNCHRONOUS_IO_NONALERT
	if openReparsePoint {
		options |= windows.FILE_OPEN_REPARSE_POINT
		// Do not overwrite file attributes. Use dispostion FILE_OPEN.
		err := ntCreateFile(&handle, access, d.handle, name.String(), windows.FILE_OPEN, options)
		if err != nil {
			// The file does not exist, so it cannot be a reparse point.
			if os.IsNotExist(err) && disposition != windows.FILE_OPEN && disposition != windows.FILE_OVERWRITE {
				return d.openNt(name, access, disposition, false)
			} else {
				return nil, err
			}
		}
		isReparsePoint, reparsePointTag, err := isReparsePointByHandle(handle)
		if err != nil {
			windows.CloseHandle(handle)
			return nil, err
		}
		if isReparsePoint {
			windows.CloseHandle(handle)
			if reparsePointTag == windows.IO_REPARSE_TAG_SYMLINK {
				return nil, syscall.ELOOP
			}
			return d.openNt(name, access, disposition, false)
		}
		if disposition != windows.FILE_OPEN {
			// Reopen with the correct disposition.
			windows.CloseHandle(handle)
			return d.openNt(name, access, disposition, false)
		}
		return os.NewFile(uintptr(handle), name.String()), nil
	}
	err := ntCreateFile(&handle, access, d.handle, name.String(), disposition, options)
	if err != nil {
		if openReparsePoint && os.IsNotExist(err) {
			return d.openNt(name, access, disposition, false)
		} else {
			return nil, err
		}
	}
	return os.NewFile(uintptr(handle), name.String()), nil
}

func (d *localDirectory) open(name path.Component, creationMode CreationMode, flag int) (*os.File, error) {
	defer runtime.KeepAlive(d)

	var access uint32
	flags := creationMode.flags | flag
	switch flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR) {
	case os.O_RDONLY:
		access = windows.FILE_GENERIC_READ
	case os.O_WRONLY:
		access = windows.FILE_GENERIC_WRITE | windows.FILE_READ_ATTRIBUTES
	case os.O_RDWR:
		access = windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE
	}
	access = access | windows.DELETE
	var disposition uint32
	switch {
	case flags&(os.O_CREATE|os.O_EXCL) == (os.O_CREATE | os.O_EXCL):
		disposition = windows.FILE_CREATE
	case flags&(os.O_CREATE|os.O_APPEND) == (os.O_CREATE | os.O_APPEND):
		disposition = windows.FILE_OPEN_IF
	case flags&os.O_CREATE == os.O_CREATE:
		disposition = windows.FILE_OVERWRITE_IF
	case flags&os.O_WRONLY == os.O_WRONLY || flags&os.O_RDWR == os.O_RDWR:
		disposition = windows.FILE_OVERWRITE
	default:
		disposition = windows.FILE_OPEN
	}
	file, err := d.openNt(name, access, disposition, true)
	if err != nil {
		return nil, err
	}
	if flags&os.O_CREATE == os.O_CREATE {
		// On NTFS, you have to explicitly set a file to be sparse.
		var returned uint32
		err = windows.DeviceIoControl(windows.Handle(file.Fd()), windows.FSCTL_SET_SPARSE, nil, 0, nil, 0, &returned, nil)
	}
	if flags&os.O_APPEND == os.O_APPEND {
		_, err = setFilePointer(windows.Handle(file.Fd()), 0, windows.FILE_END)
	}
	if err != nil {
		file.Close()
	}
	return file, err
}

func (d *localDirectory) OpenAppend(name path.Component, creationMode CreationMode) (FileAppender, error) {
	return d.open(name, creationMode, os.O_APPEND|os.O_WRONLY)
}

type localFileReadWriter struct {
	*os.File
}

func setFilePointer(handle windows.Handle, offset int64, whence uint32) (int64, error) {
	lowOrder := int32(offset & 0xffffffff)
	highOrder := int32((offset >> 32) & 0xffffffff)
	newLowOrder, err := windows.SetFilePointer(handle, lowOrder, &highOrder, whence)
	if err != nil {
		return 0, err
	}
	return (int64(highOrder) << 32) | int64(newLowOrder), nil
}

func getNextRegionOffsetSparse(handle windows.Handle, offset int64, regionType RegionType, fileSize uint64) (int64, error) {
	inBuffer := windowsext.FILE_ALLOCATED_RANGE_BUFFER{
		FileOffset: offset,
		Length:     int64(fileSize) - offset,
	}
	bufferSize := uint32(unsafe.Sizeof(inBuffer))
	initSize := uint32(512)
	outBuffer := make([]byte, initSize)
	var returned uint32
	for {
		err := windows.DeviceIoControl(handle, windows.FSCTL_QUERY_ALLOCATED_RANGES, (*byte)(unsafe.Pointer(&inBuffer)),
			bufferSize, &outBuffer[0], initSize, &returned, nil)
		if err == nil {
			break
		}
		if err.(syscall.Errno) == windows.ERROR_MORE_DATA {
			initSize *= 2
			outBuffer = make([]byte, initSize)
		} else {
			return 0, err
		}
	}
	numOutStruct := returned / bufferSize
	switch regionType {
	case Data:
		if numOutStruct == 0 {
			return 0, io.EOF
		} else {
			offset = (*windowsext.FILE_ALLOCATED_RANGE_BUFFER)(unsafe.Pointer(&outBuffer[0])).FileOffset
		}
	case Hole:
		if numOutStruct != 0 {
			lastOffset := offset
			allocRangeBufferPtr := (*windowsext.FILE_ALLOCATED_RANGE_BUFFER)(unsafe.Pointer(&outBuffer[0]))
			for i := uint32(0); i < numOutStruct; i++ {
				if allocRangeBufferPtr.FileOffset != lastOffset {
					offset = lastOffset
					break
				}
				lastOffset = allocRangeBufferPtr.FileOffset + allocRangeBufferPtr.Length
				allocRangeBufferPtr = (*windowsext.FILE_ALLOCATED_RANGE_BUFFER)(unsafe.Pointer(
					uintptr(unsafe.Pointer(allocRangeBufferPtr)) + unsafe.Sizeof(bufferSize)))
			}
			offset = lastOffset
		}
	}
	nextOffset, err := setFilePointer(handle, offset, windows.FILE_BEGIN)
	return nextOffset, err
}

func (f localFileReadWriter) GetNextRegionOffset(offset int64, regionType RegionType) (int64, error) {
	handle := windows.Handle(f.Fd())
	var fileInfo windows.ByHandleFileInformation
	err := windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return 0, err
	}
	fileAttributes := fileInfo.FileAttributes
	// Only files that have one of the two attributes have zero ranges known to the OS.
	isSparse := (fileAttributes & windows.FILE_ATTRIBUTE_SPARSE_FILE) == windows.FILE_ATTRIBUTE_SPARSE_FILE
	isCompressed := (fileAttributes & windows.FILE_ATTRIBUTE_COMPRESSED) == windows.FILE_ATTRIBUTE_COMPRESSED
	fileSize := (uint64(fileInfo.FileSizeHigh) << 32) | uint64(fileInfo.FileSizeLow)
	if offset >= int64(fileSize) {
		return 0, io.EOF
	}
	if isSparse || isCompressed {
		return getNextRegionOffsetSparse(handle, offset, regionType, fileSize)
	}
	var whence uint32
	switch regionType {
	case Data:
		whence = windows.FILE_BEGIN
	case Hole:
		offset = 0
		whence = windows.FILE_END
	}
	nextOffset, err := setFilePointer(handle, offset, whence)
	return nextOffset, err
}

func (d *localDirectory) OpenRead(name path.Component) (FileReader, error) {
	f, err := d.open(name, DontCreate, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	return localFileReadWriter{File: f}, nil
}

func (d *localDirectory) OpenReadWrite(name path.Component, creationMode CreationMode) (FileReadWriter, error) {
	f, err := d.open(name, creationMode, os.O_RDWR)
	if err != nil {
		return nil, err
	}
	return localFileReadWriter{File: f}, nil
}

func (d *localDirectory) OpenWrite(name path.Component, creationMode CreationMode) (FileWriter, error) {
	f, err := d.open(name, creationMode, os.O_WRONLY)
	if err != nil {
		return nil, err
	}
	return localFileReadWriter{File: f}, nil
}

func (d *localDirectory) Link(oldName path.Component, newDirectory Directory, newName path.Component) error {
	defer runtime.KeepAlive(d)

	return newDirectory.Apply(localDirectoryLink{
		oldHandle: d.handle,
		oldName:   oldName,
		newName:   newName,
	})
}

func (d *localDirectory) Clonefile(oldName path.Component, newDirectory Directory, newName path.Component) error {
	return status.Error(codes.Unimplemented, "Clonefile is only supported on Darwin")
}

func (d *localDirectory) lstat(name path.Component) (FileType, error) {
	var handle windows.Handle
	fileName := name.String()
	err := ntCreateFile(&handle, windows.FILE_READ_ATTRIBUTES, d.handle, fileName, windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT)
	if err != nil {
		return FileTypeOther, err
	}
	var fileInfo windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(handle, &fileInfo)
	if err != nil {
		return FileTypeOther, err
	}
	fileAttributes := fileInfo.FileAttributes
	fileType := FileTypeOther
	switch {
	case (fileAttributes & windows.FILE_ATTRIBUTE_REPARSE_POINT) == windows.FILE_ATTRIBUTE_REPARSE_POINT:
		fileType = FileTypeSymlink
	case (fileAttributes & windows.FILE_ATTRIBUTE_DIRECTORY) == windows.FILE_ATTRIBUTE_DIRECTORY:
		fileType = FileTypeDirectory
	default:
		fileType = FileTypeRegularFile
	}
	return fileType, nil
}

func (d *localDirectory) Lstat(name path.Component) (FileInfo, error) {
	defer runtime.KeepAlive(d)

	fileType, err := d.lstat(name)
	if err != nil {
		return FileInfo{}, err
	}
	// Assume all regular files are executable.
	return NewFileInfo(name, fileType, true), nil
}

func (d *localDirectory) Mkdir(name path.Component, perm os.FileMode) error {
	defer runtime.KeepAlive(d)

	var handle windows.Handle
	defer windows.CloseHandle(handle)
	// The argument perm is ignored like os.Mkdir on Windows.
	err := ntCreateFile(&handle, windows.FILE_LIST_DIRECTORY, d.handle, name.String(),
		windows.FILE_CREATE, windows.FILE_DIRECTORY_FILE|windows.FILE_OPEN_REPARSE_POINT)
	return err
}

func (d *localDirectory) Mknod(name path.Component, perm os.FileMode, deviceNumber DeviceNumber) error {
	return status.Error(codes.Unimplemented, "Creation of device nodes is not supported on Windows")
}

func readdirnames(handle windows.Handle) ([]string, error) {
	outBufferSize := uint32(512)
	outBuffer := make([]byte, outBufferSize)
	firstIteration := true
	for {
		err := windows.GetFileInformationByHandleEx(handle, windows.FileFullDirectoryInfo,
			&outBuffer[0], outBufferSize)
		if err == nil {
			break
		}
		if err.(syscall.Errno) == windows.ERROR_NO_MORE_FILES {
			if firstIteration {
				return []string{}, nil
			}
			break
		}
		if err.(syscall.Errno) == windows.ERROR_MORE_DATA {
			outBufferSize *= 2
			outBuffer = make([]byte, outBufferSize)
		} else {
			return nil, err
		}
		firstIteration = false
	}
	names := make([]string, 0)
	offset := ^(uint32(0))
	dirInfoPtr := (*windowsext.FILE_FULL_DIR_INFO)(unsafe.Pointer(&outBuffer[0]))
	for offset != 0 {
		offset = dirInfoPtr.NextEntryOffset
		fileNameLen := int(dirInfoPtr.FileNameLength) / 2
		fileNameUTF16 := make([]uint16, fileNameLen)
		targetPtr := unsafe.Pointer(&dirInfoPtr.FileName[0])
		for i := 0; i < fileNameLen; i++ {
			fileNameUTF16[i] = *(*uint16)(targetPtr)
			targetPtr = unsafe.Pointer(uintptr(targetPtr) + uintptr(2))
		}
		dirInfoPtr = (*windowsext.FILE_FULL_DIR_INFO)(unsafe.Pointer(uintptr(unsafe.Pointer(dirInfoPtr)) + uintptr(offset)))

		fileName := windows.UTF16ToString(fileNameUTF16)
		if fileName == "." || fileName == ".." {
			continue
		}
		names = append(names, fileName)
	}
	return names, nil
}

func (d *localDirectory) ReadDir() ([]FileInfo, error) {
	names, err := readdirnames(d.handle)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	list := make([]FileInfo, 0, len(names))
	for _, name := range names {
		info, err := d.Lstat(path.MustNewComponent(name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		list = append(list, info)
	}
	return list, nil
}

func (d *localDirectory) Readlink(name path.Component) (string, error) {
	var handle windows.Handle
	err := ntCreateFile(&handle, windows.FILE_GENERIC_READ, d.handle, name.String(), windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT)
	if err != nil {
		return "", err
	}
	outBufferSize := uint32(512)
	outBuffer := make([]byte, outBufferSize)
	var returned uint32
	for {
		err = windows.DeviceIoControl(handle, windows.FSCTL_GET_REPARSE_POINT, nil, 0,
			&outBuffer[0], outBufferSize, &returned, nil)
		if err == nil {
			break
		}
		if err.(syscall.Errno) == windows.ERROR_NOT_A_REPARSE_POINT {
			return "", syscall.EINVAL
		}
		if err.(syscall.Errno) == windows.ERROR_INSUFFICIENT_BUFFER {
			outBufferSize *= 2
			outBuffer = make([]byte, outBufferSize)
		} else {
			return "", err
		}
	}
	reparseDataBufferPtr := (*windowsext.REPARSE_DATA_BUFFER)(unsafe.Pointer(&outBuffer[0]))
	if reparseDataBufferPtr.ReparseTag != windows.IO_REPARSE_TAG_SYMLINK {
		return "", syscall.EINVAL
	}
	symlinkReparseBufferPtr := (*windowsext.SymbolicLinkReparseBuffer)(unsafe.Pointer(&reparseDataBufferPtr.DUMMYUNIONNAME[0]))
	contentPtr := unsafe.Pointer(uintptr(unsafe.Pointer(&symlinkReparseBufferPtr.PathBuffer[0])) + uintptr(symlinkReparseBufferPtr.SubstituteNameOffset))
	contentLen := int(symlinkReparseBufferPtr.SubstituteNameLength)
	contentUTF16 := make([]uint16, contentLen)
	for i := 0; i < contentLen; i++ {
		contentUTF16[i] = *(*uint16)(contentPtr)
		contentPtr = unsafe.Pointer(uintptr(contentPtr) + uintptr(2))
	}
	return filepath.ToSlash(windows.UTF16ToString(contentUTF16)), nil
}

func (d *localDirectory) Remove(name path.Component) error {
	isDir := false
	var handle windows.Handle
	err := ntCreateFile(&handle, windows.DELETE, d.handle, name.String(),
		windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT|windows.FILE_NON_DIRECTORY_FILE)
	if err != nil {
		if err == syscall.EISDIR {
			isDir = true
		} else {
			return err
		}
	}
	if isDir {
		err = ntCreateFile(&handle, windows.FILE_GENERIC_READ|windows.DELETE, d.handle, name.String(),
			windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT)
		if err != nil {
			return err
		}
		isReparsePoint, _, err := isReparsePointByHandle(handle)
		if err != nil {
			return err
		}
		if !isReparsePoint {
			names, err := readdirnames(handle)
			if err != nil {
				return err
			}
			if len(names) != 0 {
				return syscall.ENOTEMPTY
			}
		}
	}
	defer windows.CloseHandle(handle)
	fileDispInfo := windowsext.FILE_DISPOSITION_INFORMATION_EX{
		Flags: windows.FILE_DISPOSITION_DELETE | windows.FILE_DISPOSITION_POSIX_SEMANTICS,
	}
	var iosb windows.IO_STATUS_BLOCK
	err = windows.NtSetInformationFile(handle, &iosb, (*byte)(unsafe.Pointer(&fileDispInfo)),
		uint32(unsafe.Sizeof(fileDispInfo)), windows.FileDispositionInformationEx)
	return err
}

// On NTFS mount point is a reparse point, no need to unmount.
func (d *localDirectory) RemoveAllChildren() error {
	defer runtime.KeepAlive(d)

	names, err := readdirnames(d.handle)
	if err != nil {
		return err
	}
	for _, name := range names {
		component := path.MustNewComponent(name)
		fileType, err := d.lstat(component)
		if err != nil {
			return err
		}
		if fileType == FileTypeDirectory {
			subdirectory, err := d.enter(component, true)
			if err != nil {
				return err
			}
			err = subdirectory.RemoveAllChildren()
			subdirectory.Close()
			if err != nil {
				return err
			}
		}
		err = d.Remove(component)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *localDirectory) RemoveAll(name path.Component) error {
	defer runtime.KeepAlive(d)

	if subdirectory, err := d.EnterDirectory(name); err == nil {
		err := subdirectory.RemoveAllChildren()
		subdirectory.Close()
		if err != nil {
			return err
		}
		return d.Remove(name)
	} else if err == syscall.ENOTDIR {
		return d.Remove(name)
	} else {
		return err
	}
}

func (d *localDirectory) Rename(oldName path.Component, newDirectory Directory, newName path.Component) error {
	defer runtime.KeepAlive(d)

	return newDirectory.Apply(localDirectoryRename{
		oldHandle: d.handle,
		oldName:   oldName,
		newName:   newName,
	})
}

func buildSymlinkBuffer(target, name []uint16, isRelative bool) ([]byte, uint32) {
	targetByteSize := len(target)*2 - 2
	nameByteSize := len(name)*2 - 2
	pathBufferSize := targetByteSize + nameByteSize + 4 + 12
	bufferSize := pathBufferSize + 8
	buffer := make([]byte, bufferSize)

	reparseDataBufferPtr := (*windowsext.REPARSE_DATA_BUFFER)(unsafe.Pointer(&buffer[0]))
	reparseDataBufferPtr.ReparseTag = windows.IO_REPARSE_TAG_SYMLINK
	reparseDataBufferPtr.ReparseDataLength = uint16(pathBufferSize)

	symlinkReparseBufferPtr := (*windowsext.SymbolicLinkReparseBuffer)(unsafe.Pointer(&reparseDataBufferPtr.DUMMYUNIONNAME[0]))
	symlinkReparseBufferPtr.SubstituteNameLength = uint16(targetByteSize)
	symlinkReparseBufferPtr.PrintNameOffset = uint16(targetByteSize + 2)
	symlinkReparseBufferPtr.PrintNameLength = uint16(nameByteSize)
	var flags uint32 = 0
	if isRelative {
		flags = windowsext.SYMLINK_FLAG_RELATIVE
	}
	symlinkReparseBufferPtr.Flags = flags

	targetPtr := unsafe.Pointer(&symlinkReparseBufferPtr.PathBuffer[0])
	namePtr := unsafe.Pointer(uintptr(targetPtr) + uintptr(len(target)*2))
	copy((*[windows.MAX_LONG_PATH]uint16)(targetPtr)[:], target)
	copy((*[windows.MAX_LONG_PATH]uint16)(namePtr)[:], name)

	return buffer, uint32(bufferSize)
}

func (d *localDirectory) createNTFSSymlink(target, name string, isRelative, isDir bool) error {
	// This only works on NTFS, but this is safe to assume on Windows.
	// Also, hard link only works on NTFS, so it makes no sense to support other file systems.
	var handle windows.Handle
	var access uint32 = windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE
	var options uint32 = windows.FILE_OPEN_REPARSE_POINT
	if isDir {
		options |= windows.FILE_DIRECTORY_FILE
	}
	err := ntCreateFile(&handle, access, d.handle, name, windows.FILE_CREATE, options)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	targetUTF16, err := windows.UTF16FromString(target)
	if err != nil {
		return err
	}
	nameUTF16, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	reparseDataBuffer, bufferSize := buildSymlinkBuffer(targetUTF16, nameUTF16, isRelative)
	var returned uint32
	err = windows.DeviceIoControl(handle, windows.FSCTL_SET_REPARSE_POINT, &reparseDataBuffer[0],
		bufferSize, nil, 0, &returned, nil)
	if err != nil {
		return err
	}
	return nil
}

func getAbsPathByHandle(handle windows.Handle) (string, error) {
	len, _ := windows.GetFinalPathNameByHandle(handle, nil, 0, 0)
	buffer := make([]uint16, len)
	_, err := windows.GetFinalPathNameByHandle(handle, &buffer[0], len, 0)
	if err != nil {
		return "", err
	}
	return windows.UTF16ToString(buffer), nil
}

func (d *localDirectory) Symlink(oldName string, newName path.Component) error {
	// Creating symlinks on windows requires one of the following:
	//   1. Run as an administrator.
	//   2. Developer mode is on.
	defer runtime.KeepAlive(d)

	oldName = filepath.FromSlash(oldName)
	// Path with one leading slash (but not UNC) should also be considered absolute.
	isRelative := !(oldName[0] == '\\' || filepath.IsAbs(oldName))
	// On windows, you have to know if the target is a directory when creating a symlink.
	// If target does not exist, create file symlink like os.Symlink.
	var isDir bool
	var targetPath string
	if isRelative {
		cleanRelPath, err := filepath.Rel(".", oldName)
		if err != nil {
			return err
		}
		quickReturn := false
		// If target is a child, we can check attribute using handle.
		if cleanRelPath == "." || cleanRelPath == ".." {
			quickReturn = true
			isDir = true
		} else if !strings.HasPrefix(cleanRelPath, "..\\") {
			quickReturn = true
			var handle windows.Handle
			err := ntCreateFile(&handle, windows.FILE_READ_ATTRIBUTES, d.handle, cleanRelPath, windows.FILE_OPEN, windows.FILE_DIRECTORY_FILE)
			if err != nil {
				if err == syscall.ENOTDIR || os.IsNotExist(err) {
					isDir = false
				} else {
					return err
				}
			} else {
				isDir = true
				windows.CloseHandle(handle)
			}
		}
		if quickReturn {
			return d.createNTFSSymlink(cleanRelPath, newName.String(), isRelative, isDir)
		}
		handlePath, err := getAbsPathByHandle(d.handle)
		if err != nil {
			return err
		}
		targetPath = filepath.Join(handlePath, cleanRelPath)
	} else {
		targetPath = oldName
		// Fix paths like C:\ for NT namespace.
		if oldName[0] != '\\' {
			oldName = "\\??\\" + oldName
		}
	}
	fi, err := os.Stat(targetPath)
	isDir = err == nil && fi.IsDir()
	return d.createNTFSSymlink(oldName, newName.String(), isRelative, isDir)
}

func (d *localDirectory) Sync() error {
	defer runtime.KeepAlive(d)

	return windows.FlushFileBuffers(d.handle)
}

func (d *localDirectory) Chtimes(name path.Component, atime, mtime time.Time) error {
	var handle windows.Handle
	err := ntCreateFile(&handle, windows.FILE_GENERIC_READ|windows.FILE_WRITE_ATTRIBUTES, d.handle, name.String(),
		windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	aFileTime := windows.NsecToFiletime(atime.UnixNano())
	mFileTime := windows.NsecToFiletime(mtime.UnixNano())
	err = windows.SetFileTime(handle, nil, &aFileTime, &mFileTime)
	return err
}

func (d *localDirectory) IsWritable() (bool, error) {
	// If you can enter the directory, you can write.
	// Permission is ignored by Mkdir().
	return true, nil
}

func (d *localDirectory) IsWritableChild(name path.Component) (bool, error) {
	return true, nil
}

func haveSameUnderlyingFile(left, right windows.Handle) (res bool, err error) {
	if uintptr(left) == uintptr(right) {
		return true, nil
	}
	var leftInfo windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(left, &leftInfo)
	if err != nil {
		return
	}
	var rightInfo windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(right, &rightInfo)
	if err != nil {
		return
	}
	res = leftInfo.VolumeSerialNumber == rightInfo.VolumeSerialNumber &&
		leftInfo.FileIndexLow == rightInfo.FileIndexLow &&
		leftInfo.FileIndexHigh == rightInfo.FileIndexHigh
	return
}

func buildFileLinkInfo(root windows.Handle, name []uint16) ([]byte, uint32) {
	fileNameLen := len(name)*2 - 2
	bufferSize := int(unsafe.Offsetof(windowsext.FILE_LINK_INFORMATION{}.FileName)) + fileNameLen + 2
	buffer := make([]byte, bufferSize)
	typedBufferPtr := (*windowsext.FILE_LINK_INFORMATION)(unsafe.Pointer(&buffer[0]))

	typedBufferPtr.RootDirectory = root
	typedBufferPtr.FileNameLength = uint32(fileNameLen)
	copy((*[windows.MAX_LONG_PATH]uint16)(unsafe.Pointer(&typedBufferPtr.FileName[0]))[:], name)

	return buffer, uint32(bufferSize)
}

func createNTFSHardlink(oldHandle windows.Handle, oldName string, newHandle windows.Handle, newName string) error {
	var handle windows.Handle
	err := ntCreateFile(&handle, windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE, oldHandle, oldName, windows.FILE_OPEN, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	newNameUTF16, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	var linkRoot windows.Handle
	areSame, err := haveSameUnderlyingFile(oldHandle, newHandle)
	if err != nil {
		return err
	}
	if areSame {
		linkRoot = windows.Handle(uintptr(0))
	} else {
		linkRoot = newHandle
	}
	fileLinkInfo, bufferSize := buildFileLinkInfo(linkRoot, newNameUTF16)
	var iosb windows.IO_STATUS_BLOCK
	ntstatus := windows.NtSetInformationFile(handle, &iosb, &fileLinkInfo[0], bufferSize, windows.FileLinkInformation)
	if ntstatus != nil {
		return ntstatus.(windows.NTStatus).Errno()
	}
	return nil
}

func renameHelper(sourceHandle, newHandle windows.Handle, newName string) (areSame bool, err error) {
	// We want to know a few things before renaming:
	//  1. Are source and target hard links to the same file? If so, noop.
	//  2. If target exists and wither source or target is a directory, don't overwrite and report error.
	//  3. If neither is the case, move and, if necessary, replace.
	var targetHandle windows.Handle
	err = ntCreateFile(&targetHandle, windows.FILE_READ_ATTRIBUTES, newHandle, newName, windows.FILE_OPEN,
		windows.FILE_OPEN_REPARSE_POINT)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	defer windows.CloseHandle(targetHandle)
	var targetInfo windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(targetHandle, &targetInfo)
	if err != nil {
		return
	}
	var sourceInfo windows.ByHandleFileInformation
	err = windows.GetFileInformationByHandle(sourceHandle, &sourceInfo)
	if err != nil {
		return
	}
	if (targetInfo.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY) == windows.FILE_ATTRIBUTE_DIRECTORY ||
		(sourceInfo.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY) == windows.FILE_ATTRIBUTE_DIRECTORY {
		err = syscall.EEXIST
		return
	}
	areSame = targetInfo.VolumeSerialNumber == sourceInfo.VolumeSerialNumber &&
		targetInfo.FileIndexLow == sourceInfo.FileIndexLow &&
		targetInfo.FileIndexHigh == sourceInfo.FileIndexHigh
	return
}

func buildFileRenameInfo(root windows.Handle, name []uint16) ([]byte, uint32) {
	fileNameLen := len(name)*2 - 2
	bufferSize := int(unsafe.Offsetof(windowsext.FILE_RENAME_INFORMATION{}.FileName)) + fileNameLen
	buffer := make([]byte, bufferSize)
	typedBufferPtr := (*windowsext.FILE_RENAME_INFORMATION)(unsafe.Pointer(&buffer[0]))

	typedBufferPtr.ReplaceIfExists = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	typedBufferPtr.RootDirectory = root
	typedBufferPtr.FileNameLength = uint32(fileNameLen)
	copy((*[windows.MAX_LONG_PATH]uint16)(unsafe.Pointer(&typedBufferPtr.FileName[0]))[:], name)

	return buffer, uint32(bufferSize)
}

func rename(oldHandle windows.Handle, oldName string, newHandle windows.Handle, newName string) error {
	var handle windows.Handle
	err := ntCreateFile(&handle, windows.FILE_GENERIC_WRITE|windows.FILE_GENERIC_READ|windows.DELETE, oldHandle, oldName,
		windows.FILE_OPEN, windows.FILE_OPEN_REPARSE_POINT)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(handle)
	hardLinkToSameFile, err := renameHelper(handle, newHandle, newName)
	if err != nil {
		return err
	}
	if hardLinkToSameFile {
		return nil
	}
	newNameUTF16, err := windows.UTF16FromString(newName)
	if err != nil {
		return err
	}
	areSame, err := haveSameUnderlyingFile(oldHandle, newHandle)
	if err != nil {
		return err
	}
	var renameRoot windows.Handle
	if areSame {
		renameRoot = windows.Handle(uintptr(0))
	} else {
		renameRoot = newHandle
	}
	fileRenameInfo, bufferSize := buildFileRenameInfo(renameRoot, newNameUTF16)
	var iosb windows.IO_STATUS_BLOCK
	ntstatus := windows.NtSetInformationFile(handle, &iosb, &fileRenameInfo[0], bufferSize, windows.FileRenameInformation)
	if ntstatus != nil {
		return ntstatus.(windows.NTStatus).Errno()
	}
	return nil
}

type localDirectoryLink struct {
	oldHandle windows.Handle
	oldName   path.Component
	newName   path.Component
}

type localDirectoryRename struct {
	oldHandle windows.Handle
	oldName   path.Component
	newName   path.Component
}

func (d *localDirectory) Apply(arg interface{}) error {
	switch a := arg.(type) {
	case localDirectoryLink:
		defer runtime.KeepAlive(d)
		return createNTFSHardlink(a.oldHandle, a.oldName.String(), d.handle, a.newName.String())
	case localDirectoryRename:
		defer runtime.KeepAlive(d)
		return rename(a.oldHandle, a.oldName.String(), d.handle, a.newName.String())
	default:
		return syscall.EXDEV
	}
}

func (d *localDirectory) Mount(mountpoint path.Component, source, fstype string) error {
	return status.Error(codes.Unimplemented, "Mount is not supported")
}

func (d *localDirectory) Unmount(mountpoint path.Component) error {
	return status.Error(codes.Unimplemented, "Unmount is not supported")
}
