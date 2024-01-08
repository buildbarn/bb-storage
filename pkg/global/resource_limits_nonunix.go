//go:build windows
// +build windows

package global

import (
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/global"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func setResourceLimit(name string, resourceLimit *global.SetResourceLimitConfiguration) error {
	return status.Error(codes.Unimplemented, "Resource limits cannot be adjusted on this operating system")
}
