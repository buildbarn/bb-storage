// +build darwin

package configuration

import (
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func memoryMapBlockDevice(path string) (local.ReadWriterAt, int, int64, error) {
	return nil, 0, 0, status.Error(codes.Unimplemented, "Memory mapping block devices is not supported on this platform")
}
