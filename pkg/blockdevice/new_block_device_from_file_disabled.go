// +build windows

package blockdevice

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewBlockDeviceFromFile creates a BlockDevice that is backed by a
// regular file stored in a file system. This implementation is a stub
// for operating systems that don't support block device access.
func NewBlockDeviceFromFile(path string, minimumSizeBytes int) (BlockDevice, int, int64, error) {
	return nil, 0, 0, status.Error(codes.Unimplemented, "Memory mapping block devices is not supported on this platform")
}
