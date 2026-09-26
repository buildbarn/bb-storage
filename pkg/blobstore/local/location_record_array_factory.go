package local

import (
	"github.com/buildbarn/bb-storage/pkg/blockdevice"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
)

type locationRecordArrayFactory struct{}

// LocationRecordArrayFactory can be passed to
// lossymap.NewHashMapFromConfiguration() to create a
// LocationRecordArray that is either stored in memory or backed by a
// block device.
var LocationRecordArrayFactory lossymap.RecordArrayFactory[Key, Location, BlockReferenceResolver] = locationRecordArrayFactory{}

func (locationRecordArrayFactory) GetBlockDeviceBackedRecordSize() int {
	return blockDeviceBackedLocationRecordSize
}

func (locationRecordArrayFactory) NewInMemoryRecordArray(entries int) LocationRecordArray {
	return NewInMemoryLocationRecordArray(entries)
}

func (locationRecordArrayFactory) NewBlockDeviceBackedRecordArray(blockDevice blockdevice.BlockDevice) LocationRecordArray {
	return NewBlockDeviceBackedLocationRecordArray(blockDevice)
}
