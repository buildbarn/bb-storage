package filesystem

import (
	"os"
)

type simpleFileInfo struct {
	name string
	mode os.FileMode
}

// NewSimpleFileInfo constructs a FileInfo object that returns fixed
// values for its methods.
func NewSimpleFileInfo(name string, mode os.FileMode) FileInfo {
	return &simpleFileInfo{
		name: name,
		mode: mode,
	}
}

func (fi *simpleFileInfo) Name() string {
	return fi.name
}

func (fi *simpleFileInfo) Mode() os.FileMode {
	return fi.mode
}
