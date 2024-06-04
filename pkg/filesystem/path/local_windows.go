//go:build windows

package path

import (
	"path/filepath"
)

// NewLocalParser creates a pathname parser for paths that are native to
// the locally running operating system.
func NewLocalParser(path string) (Parser, error) {
	return NewWindowsParser(filepath.ToSlash(path))
}

// GetLocalString converts a path to a string representation that is
// supported by the locally running operating system.
func GetLocalString(s Stringer) (string, error) {
	return s.GetWindowsString()
}
