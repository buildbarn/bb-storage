package blockdevice

import (
	pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blockdevice"

	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewBlockDeviceFromConfiguration creates a BlockDevice based on
// parameters provided in a configuration file.
func NewBlockDeviceFromConfiguration(configuration *pb.Configuration, mayZeroInitialize bool) (BlockDevice, int, int64, error) {
	if configuration == nil {
		return nil, 0, 0, status.Error(codes.InvalidArgument, "Block device configuration not specified")
	}

	var blockDevice BlockDevice
	var sectorSizeBytes int
	var sectorCount int64
	switch source := configuration.Source.(type) {
	case *pb.Configuration_DevicePath:
		var err error
		blockDevice, sectorSizeBytes, sectorCount, err = NewBlockDeviceFromDevice(source.DevicePath)
		if err != nil {
			return nil, 0, 0, err
		}
	case *pb.Configuration_File:
		var err error
		blockDevice, sectorSizeBytes, sectorCount, err = NewBlockDeviceFromFile(source.File.Path, int(source.File.SizeBytes), mayZeroInitialize)
		if err != nil {
			return nil, 0, 0, err
		}
	default:
		return nil, 0, 0, status.Error(codes.InvalidArgument, "Configuration did not contain a supported block device source")
	}

	if limit := configuration.WriteConcurrencyLimit; limit > 0 {
		blockDevice = NewWriteConcurrencyLimitingBlockDevice(
			blockDevice,
			semaphore.NewWeighted(limit),
		)
	}
	return blockDevice, sectorSizeBytes, sectorCount, nil
}
