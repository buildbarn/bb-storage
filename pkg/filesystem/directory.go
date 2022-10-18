package filesystem

import (
	"io"
	"os"
	"time"

	"github.com/buildbarn/bb-storage/pkg/filesystem/path"
)

// DeterministicFileModificationTimestamp is a fixed timestamp that can
// be provided to Directory.Chtimes() to give files deterministic
// modification times. It is used by bb_worker to ensure that all files
// in the input root of a build action have the same modification time.
// This is needed to make certain kinds of build actions (most notably
// Autoconf scripts) succeed.
//
// 2000-01-01T00:00:00Z was chosen, because it's easy to distinguish
// from genuine timestamps. 1970-01-01T00:00:00Z would be impractical to
// use, because it tends to cause regressions in practice.
//
// Examples:
// https://bugs.python.org/issue34097
// https://gerrit.wikimedia.org/r/#/c/mediawiki/core/+/437977/
var DeterministicFileModificationTimestamp = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// CreationMode specifies whether and how Directory.Open*() should
// create new files.
type CreationMode struct {
	flags       int
	permissions os.FileMode
}

// DontCreate indicates that opening should fail in case the target file
// does not exist.
var DontCreate = CreationMode{}

// CreateReuse indicates that a new file should be created if it doesn't
// already exist. If the target file already exists, that file will be
// opened instead.
func CreateReuse(perm os.FileMode) CreationMode {
	return CreationMode{flags: os.O_CREATE, permissions: perm}
}

// CreateExcl indicates that a new file should be created. If the target
// file already exists, opening shall fail.
func CreateExcl(perm os.FileMode) CreationMode {
	return CreationMode{flags: os.O_CREATE | os.O_EXCL, permissions: perm}
}

// Directory is an abstraction for accessing a subtree of the file
// system. Each of the functions should be implemented in such a way
// that they reject access to data stored outside of the subtree. This
// allows for safe, race-free traversal of the file system.
//
// By placing this in a separate interface, it's easier to stub out file
// system handling as part of unit tests entirely.
type Directory interface {
	// EnterDirectory creates a derived directory handle for a
	// subdirectory of the current subtree.
	EnterDirectory(name path.Component) (DirectoryCloser, error)

	// Open a file contained within the directory for writing, only
	// allowing data to be appended to the end of the file.
	OpenAppend(name path.Component, creationMode CreationMode) (FileAppender, error)
	// Open a file contained within the directory for reading. The
	// CreationMode is assumed to be equal to DontCreate.
	OpenRead(name path.Component) (FileReader, error)
	// Open a file contained within the current directory for both
	// reading and writing.
	OpenReadWrite(name path.Component, creationMode CreationMode) (FileReadWriter, error)
	// Open a file contained within the current directory for writing.
	OpenWrite(name path.Component, creationMode CreationMode) (FileWriter, error)

	// Link is the equivalent of os.Link().
	Link(oldName path.Component, newDirectory Directory, newName path.Component) error
	// Clonefile is the equivalent of unix.Clonefile on macOS.
	Clonefile(oldName path.Component, newDirectory Directory, newName path.Component) error
	// Lstat is the equivalent of os.Lstat().
	Lstat(name path.Component) (FileInfo, error)
	// Mkdir is the equivalent of os.Mkdir().
	Mkdir(name path.Component, perm os.FileMode) error
	// Mknod is the equivalent of unix.Mknod().
	Mknod(name path.Component, perm os.FileMode, deviceNumber DeviceNumber) error
	// ReadDir is the equivalent of os.ReadDir().
	ReadDir() ([]FileInfo, error)
	// Readlink is the equivalent of os.Readlink().
	Readlink(name path.Component) (string, error)
	// Remove is the equivalent of os.Remove().
	Remove(name path.Component) error
	// RemoveAll is the equivalent of os.RemoveAll().
	RemoveAll(name path.Component) error
	// RemoveAllChildren empties out a directory, without removing
	// the directory itself.
	RemoveAllChildren() error
	// Rename is the equivalent of os.Rename().
	Rename(oldName path.Component, newDirectory Directory, newName path.Component) error
	// Symlink is the equivalent of os.Symlink().
	Symlink(oldName string, newName path.Component) error
	// Sync the contents of a directory (i.e., the list of names) to
	// disk. This does not sync the contents of the files
	// themselves.
	Sync() error
	// Chtimes sets the atime and mtime of the named file.
	Chtimes(name path.Component, atime, mtime time.Time) error

	// IsWritable checks whether the Directory can be written to by the current user.
	IsWritable() (bool, error)
	// IsWritableChild checks whether the path in the Directory can be written to by the current user.
	IsWritableChild(name path.Component) (bool, error)

	// Function that base types may use to implement calls that
	// require double dispatching, such as hardlinking and renaming.
	Apply(arg interface{}) error
}

// DirectoryCloser is a Directory handle that can be released.
type DirectoryCloser interface {
	Directory
	io.Closer
}

type nopDirectoryCloser struct {
	Directory
}

// NopDirectoryCloser adds a no-op Close method to a Directory object,
// similar to how io.NopCloser() adds a Close method to a Reader.
func NopDirectoryCloser(d Directory) DirectoryCloser {
	return nopDirectoryCloser{
		Directory: d,
	}
}

func (d nopDirectoryCloser) Close() error {
	return nil
}
