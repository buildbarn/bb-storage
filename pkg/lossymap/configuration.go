package lossymap

import (
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	configuration_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/lossymap"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecordArrayFactory is called into by NewHashMapFromConfiguration() to
// create record arrays that back the resulting hash map.
type RecordArrayFactory[TKey, TValue, TExpirationData any] interface {
	GetBlockDeviceBackedRecordSize() int
	NewInMemoryRecordArray(entries int) RecordArray[TKey, TValue, TExpirationData]
	NewBlockDeviceBackedRecordArray(blockDevice blockdevice.BlockDevice) RecordArray[TKey, TValue, TExpirationData]
}

// NewHashMapFromConfiguration creates a lossy hash map using the
// parameters specified in a configuration file.
func NewHashMapFromConfiguration[TKey comparable, TValue, TExpirationData any](
	configuration *configuration_pb.HashMapConfiguration,
	name string,
	recordArrayFactory RecordArrayFactory[TKey, TValue, TExpirationData],
	recordKeyHasher RecordKeyHasher[TKey],
	valueComparator ValueComparator[TValue],
	persistent bool,
) (Map[TKey, TValue, TExpirationData], error) {
	if configuration == nil {
		return nil, status.Error(codes.InvalidArgument, "No configuration provided")
	}

	if configuration.MaximumGetAttempts < 1 {
		return nil, status.Error(codes.InvalidArgument, "Maximum get attempts must be positive")
	}
	if configuration.MaximumPutAttempts < 1 {
		return nil, status.Error(codes.InvalidArgument, "Maximum put attempts must be positive")
	}

	// Create the record array that backs the hash map.
	var recordCount uint64
	var recordArray RecordArray[TKey, TValue, TExpirationData]
	switch backendConfiguration := configuration.Backend.(type) {
	case *configuration_pb.HashMapConfiguration_InMemory_:
		recordCount = backendConfiguration.InMemory.Entries
		recordArray = recordArrayFactory.NewInMemoryRecordArray(int(recordCount))
	case *configuration_pb.HashMapConfiguration_OnBlockDevice:
		blockDevice, sectorSizeBytes, sectorCount, err := blockdevice.NewBlockDeviceFromConfiguration(
			backendConfiguration.OnBlockDevice,
			/* mayZeroInitialize = */ !persistent,
		)
		if err != nil {
			return nil, util.StatusWrap(err, "Failed to create block device")
		}
		recordCount = uint64(sectorSizeBytes) * uint64(sectorCount) / uint64(recordArrayFactory.GetBlockDeviceBackedRecordSize())
		recordArray = recordArrayFactory.NewBlockDeviceBackedRecordArray(blockDevice)
	default:
		return nil, status.Error(codes.InvalidArgument, "No backend provided")
	}

	return NewHashMap(
		recordArray,
		recordKeyHasher,
		recordCount,
		valueComparator,
		uint8(configuration.MaximumGetAttempts),
		int(configuration.MaximumPutAttempts),
		name,
	), nil
}
