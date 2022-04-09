package filesystem

import (
	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// FileType is an enumeration of the type of a file stored on a file
// system.
type FileType int

const (
	// FileTypeRegularFile means the file is a regular file.
	FileTypeRegularFile FileType = iota
	// FileTypeDirectory means the file is a directory.
	FileTypeDirectory
	// FileTypeSymlink means the file is a symbolic link.
	FileTypeSymlink
	// FileTypeBlockDevice means the file is a block device.
	FileTypeBlockDevice
	// FileTypeCharacterDevice means the file is a character device.
	FileTypeCharacterDevice
	// FileTypeFIFO means the file is a FIFO.
	FileTypeFIFO
	// FileTypeSocket means the file is a socket.
	FileTypeSocket
	// FileTypeOther means the file is neither a regular file, a
	// directory or symbolic link.
	FileTypeOther
)

// FileInfo is a subset of os.FileInfo, only containing the features
// used by the Buildbarn codebase.
type FileInfo struct {
	name         path.Component
	fileType     FileType
	isExecutable bool
}

// NewFileInfo constructs a FileInfo object that returns fixed values
// for its methods.
func NewFileInfo(name path.Component, fileType FileType, isExecutable bool) FileInfo {
	return FileInfo{
		name:         name,
		fileType:     fileType,
		isExecutable: isExecutable,
	}
}

// Name returns the filename of the file.
func (fi *FileInfo) Name() path.Component {
	return fi.name
}

// Type returns the type of a file (e.g., regular file, directory, symlink).
func (fi *FileInfo) Type() FileType {
	return fi.fileType
}

// IsExecutable returns whether the regular file is executable.
func (fi *FileInfo) IsExecutable() bool {
	return fi.isExecutable
}

// FileInfoList is a list of FileInfo objects returned by
// Directory.ReadDir(). This type may be used to sort
// elements in the list by name.
type FileInfoList []FileInfo

func (l FileInfoList) Len() int {
	return len(l)
}

func (l FileInfoList) Less(i, j int) bool {
	return l[i].Name().String() < l[j].Name().String()
}

func (l FileInfoList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
