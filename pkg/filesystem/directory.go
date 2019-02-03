package filesystem

import (
	"os"
)

// Directory is an abstraction for accessing a subtree of the file
// system. Each of the functions should be implemented in such a way
// that they reject access to data stored outside of the subtree. This
// allows for safe, race-free traversal of the file system.
//
// By placing this in a separate interface, it's easier to stub out file
// system handling as part of unit tests entirely.
type Directory interface {
	// Enter creates a derived directory handle for a subdirectory
	// of the current subtree.
	Enter(name string) (Directory, error)
	// Close any resources associated with the current directory.
	Close() error

	// Link is the equivalent of os.Link().
	Link(oldName string, newDirectory Directory, newName string) error
	// Lstat is the equivalent of os.Lstat().
	Lstat(name string) (FileInfo, error)
	// Mkdir is the equivalent of os.Mkdir().
	Mkdir(name string, perm os.FileMode) error
	// OpenFile is the equivalent of os.OpenFile().
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// ReadDir is the equivalent of ioutil.ReadDir().
	ReadDir() ([]FileInfo, error)
	// Readlink is the equivalent of os.Readlink().
	Readlink(name string) (string, error)
	// Remove is the equivalent of os.Remove().
	Remove(name string) error
	// RemoveAll is the equivalent of os.RemoveAll().
	RemoveAll(name string) error
	// RemoveAllChildren empties out a directory, without removing
	// the directory itself.
	RemoveAllChildren() error
	// Symlink is the equivalent of os.Symlink().
	Symlink(oldName string, newName string) error
}
