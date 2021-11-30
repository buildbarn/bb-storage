//go:build windows
// +build windows

package global

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setUmask(umask uint32) error {
	return status.Error(codes.InvalidArgument, "This platform does not permit setting the umask")
}
