// +build windows

package filesystem

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewLocalDirectory creates a directory handle that corresponds to a
// local path on the system. On this operating system this functionality
// is not available.
func NewLocalDirectory(path string) (DirectoryCloser, error) {
	return nil, status.Error(codes.Unimplemented, "Local file system access is not supported on this platform")
}
