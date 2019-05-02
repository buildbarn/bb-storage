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
