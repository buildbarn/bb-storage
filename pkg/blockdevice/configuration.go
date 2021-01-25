package blockdevice

import (
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blockdevice"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewBlockDeviceFromConfiguration creates a BlockDevice based on
// parameters provided in a configuration file.
func NewBlockDeviceFromConfiguration(configuration *pb.Configuration, mayZeroInitialize bool) (BlockDevice, int, int64, error) {
	if configuration == nil {
		return nil, 0, 0, status.Error(codes.InvalidArgument, "Block device configuration not specified")
	}

	switch source := configuration.Source.(type) {
	case *pb.Configuration_DevicePath:
		return NewBlockDeviceFromDevice(source.DevicePath)
	case *pb.Configuration_File:
		return NewBlockDeviceFromFile(source.File.Path, int(source.File.SizeBytes), mayZeroInitialize)
	default:
		return nil, 0, 0, status.Error(codes.InvalidArgument, "Configuration did not contain a supported block device source")
	}
}
