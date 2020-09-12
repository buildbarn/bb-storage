package local

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/blockdevice"
)

const (
	// BlockDeviceBackedLocationRecordSize is the size of a single
	// serialized LocationRecord in bytes. In serialized form, a
	// LocationRecord contains the following fields:
	//
	// - Epoch ID                     4 bytes
	// - Blocks from last             2 bytes
	// - Key:                        32 bytes
	// - Hash table probing attempt   4 bytes
	// - Blob offset                  8 bytes
	// - Blob length                  8 bytes
	// - Record checksum              8 bytes
	//                        Total: 66 bytes
	BlockDeviceBackedLocationRecordSize = 4 + 2 + sha256.Size + 4 + 8 + 8 + 8
)

type blockDeviceBackedLocationRecordArray struct {
	device   blockdevice.BlockDevice
	resolver BlockReferenceResolver
}

// NewBlockDeviceBackedLocationRecordArray creates a persistent
// LocationRecordArray. It works by using a block device as an
// array-like structure, writing serialized LocationRecords next to each
// other.
func NewBlockDeviceBackedLocationRecordArray(device blockdevice.BlockDevice, resolver BlockReferenceResolver) LocationRecordArray {
	return &blockDeviceBackedLocationRecordArray{
		device:   device,
		resolver: resolver,
	}
}

// computeChecksumForRecord computes an FNV-1a hash of all the fields in
// a serialized LocationRecord, using a hash initialization that
// corresponds to that of the epoch ID.
func computeChecksumForRecord(record *[BlockDeviceBackedLocationRecordSize]byte, h uint64) uint64 {
	for i := 4 + 2; i < 4+2+sha256.Size+4+8+8; i++ {
		h ^= uint64(record[i])
		h *= 1099511628211
	}
	return h
}

func (lra *blockDeviceBackedLocationRecordArray) Get(index int) (LocationRecord, error) {
	var record [BlockDeviceBackedLocationRecordSize]byte
	if _, err := lra.device.ReadAt(record[:], int64(index)*BlockDeviceBackedLocationRecordSize); err != nil {
		return LocationRecord{}, err
	}

	// Reobtain the index of the block in the BlockList. This may
	// fail if the entry refers to a block that is no longer there.
	blockIndex, hashSeed, found := lra.resolver.BlockReferenceToBlockIndex(BlockReference{
		EpochID:        binary.LittleEndian.Uint32(record[:]),
		BlocksFromLast: binary.LittleEndian.Uint16(record[4:]),
	})
	if !found {
		return LocationRecord{}, ErrLocationRecordInvalid
	}

	// Discard entries for which the checksum of the record doesn't
	// match up with what's expected. Such records may have either
	// been corrupted or correspond to blobs that weren't flushed
	// before shutdown.
	if computeChecksumForRecord(&record, hashSeed) != binary.LittleEndian.Uint64(record[4+2+sha256.Size+4+8+8:]) {
		return LocationRecord{}, ErrLocationRecordInvalid
	}

	// Deserialize the read record into a LocationRecord.
	l := LocationRecord{
		RecordKey: LocationRecordKey{
			Attempt: binary.LittleEndian.Uint32(record[4+2+sha256.Size:]),
		},
		Location: Location{
			BlockIndex:  blockIndex,
			OffsetBytes: int64(binary.LittleEndian.Uint64(record[4+2+sha256.Size+4:])),
			SizeBytes:   int64(binary.LittleEndian.Uint64(record[4+2+sha256.Size+4+8:])),
		},
	}
	copy(l.RecordKey.Key[:], record[4+2:])
	return l, nil
}

func (lra *blockDeviceBackedLocationRecordArray) Put(index int, locationRecord LocationRecord) error {
	blockReference, hashSeed := lra.resolver.BlockIndexToBlockReference(locationRecord.Location.BlockIndex)

	// Serialize the LocationRecord ready to be written to disk.
	var record [BlockDeviceBackedLocationRecordSize]byte
	binary.LittleEndian.PutUint32(record[:], blockReference.EpochID)
	binary.LittleEndian.PutUint16(record[4:], blockReference.BlocksFromLast)
	copy(record[4+2:], locationRecord.RecordKey.Key[:])
	binary.LittleEndian.PutUint32(record[4+2+sha256.Size:], locationRecord.RecordKey.Attempt)
	binary.LittleEndian.PutUint64(record[4+2+sha256.Size+4:], uint64(locationRecord.Location.OffsetBytes))
	binary.LittleEndian.PutUint64(record[4+2+sha256.Size+4+8:], uint64(locationRecord.Location.SizeBytes))
	binary.LittleEndian.PutUint64(record[4+2+sha256.Size+4+8+8:], computeChecksumForRecord(&record, hashSeed))

	_, err := lra.device.WriteAt(record[:], int64(index)*BlockDeviceBackedLocationRecordSize)
	return err
}
