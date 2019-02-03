package filesystem

import (
	"os"
)

// FileInfo is a subset of os.FileInfo, only containing the features
// used by the Buildbarn codebase.
type FileInfo interface {
	Name() string
	Mode() os.FileMode
}
