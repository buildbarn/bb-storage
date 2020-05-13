// +build darwin

package blockdevice

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MemoryMapBlockDevice maps the entire contents of a block device into
// the address space of the current process. This implementation is a
// stub for operating systems that don't support block device access.
func MemoryMapBlockDevice(path string) (ReadWriterAt, int, int64, error) {
	return nil, 0, 0, status.Error(codes.Unimplemented, "Memory mapping block devices is not supported on this platform")
}
