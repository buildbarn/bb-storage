package filesystem

// FileType is an enumeration of the type of a file stored on a file
// system.
type FileType int

const (
	// FileTypeRegularFile means the file is a regular file that is
	// not executable.
	FileTypeRegularFile FileType = iota
	// FileTypeExecutableFile means the file is a regular file that
	// is executable.
	FileTypeExecutableFile
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
	name     string
	fileType FileType
}

// NewFileInfo constructs a FileInfo object that returns fixed values
// for its methods.
func NewFileInfo(name string, fileType FileType) FileInfo {
	return FileInfo{
		name:     name,
		fileType: fileType,
	}
}

// Name returns the filename of the file.
func (fi *FileInfo) Name() string {
	return fi.name
}

// Type returns the type of a file (e.g., regular file, directory, symlink).
func (fi *FileInfo) Type() FileType {
	return fi.fileType
}

// FileInfoList is a list of FileInfo objects returned by
// Directory.ReadDir(). This type may be used to sort
// elements in the list by name.
type FileInfoList []FileInfo

func (l FileInfoList) Len() int {
	return len(l)
}

func (l FileInfoList) Less(i, j int) bool {
	return l[i].Name() < l[j].Name()
}

func (l FileInfoList) Swap(i, j int) {
	t := l[i]
	l[i] = l[j]
	l[j] = t
}
