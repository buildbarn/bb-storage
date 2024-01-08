//go:build darwin || freebsd || linux
// +build darwin freebsd linux

package global

import (
	"github.com/buildbarn/bb-storage/pkg/proto/configuration/global"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func convertResourceLimitValue(limit *wrapperspb.UInt64Value) resourceLimitValueType {
	if limit == nil {
		// No limit provided. Assume infinity.
		return unix.RLIM_INFINITY
	}
	return resourceLimitValueType(limit.Value)
}

// setResourceLimit applies a single resource limit that is provided in
// the configuration file against the current process using
// setrlimit(2).
func setResourceLimit(name string, resourceLimit *global.SetResourceLimitConfiguration) error {
	resource, ok := resourceLimitNames[name]
	if !ok {
		return status.Error(codes.InvalidArgument, "Resource name is not supported by this operating system")
	}
	return unix.Setrlimit(resource, &unix.Rlimit{
		Cur: convertResourceLimitValue(resourceLimit.SoftLimit),
		Max: convertResourceLimitValue(resourceLimit.HardLimit),
	})
}
