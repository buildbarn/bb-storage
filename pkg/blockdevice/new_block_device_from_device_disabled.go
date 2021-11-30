//go:build darwin || windows
// +build darwin windows

package blockdevice

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewBlockDeviceFromDevice maps the entire contents of a block device
// into the address space of the current process. This implementation is
// a stub for operating systems that don't support block device access.
func NewBlockDeviceFromDevice(path string) (BlockDevice, int, int64, error) {
	return nil, 0, 0, status.Error(codes.Unimplemented, "Memory mapping block devices is not supported on this platform")
}
