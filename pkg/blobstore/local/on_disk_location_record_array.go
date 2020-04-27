package local

import (
	"encoding/binary"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/filesystem"
)

func serializeLocationRecord(locationRecord LocationRecord) []byte {
	serialised := make([]byte, 60)
	serialised = append(serialised, locationRecord.Key.Digest[0:]...)
	binary.LittleEndian.PutUint32(serialised[32:36], locationRecord.Key.Attempt)
	binary.LittleEndian.PutUint64(serialised[36:44], uint64(locationRecord.Location.BlockID))
	binary.LittleEndian.PutUint64(serialised[44:52], uint64(locationRecord.Location.OffsetBytes))
	binary.LittleEndian.PutUint64(serialised[52:60], uint64(locationRecord.Location.SizeBytes))

	return serialised
}

func deserializeLocationRecord(record []byte) LocationRecord {
	var keyDigest [32]byte
	copy(keyDigest[0:32], record[0:32])
	keyAttempt := binary.LittleEndian.Uint32(record[32:36])
	locationBlockId := int(binary.LittleEndian.Uint64(record[36:44]))
	locationOffset := int64(binary.LittleEndian.Uint64(record[44:52]))
	locationSize := int64(binary.LittleEndian.Uint64(record[52:60]))

	key := LocationRecordKey{
		Digest:  keyDigest,
		Attempt: keyAttempt,
	}

	location := Location{
		BlockID:     locationBlockId,
		OffsetBytes: locationOffset,
		SizeBytes:   locationSize,
	}

	return LocationRecord{
		Key:      key,
		Location: location,
	}
}

type onDiskLocationRecordArray struct {
	recordFile filesystem.FileReadWriter
	lock       sync.Mutex
}

func NewOnDiskLocationRecordArray(recordFile filesystem.FileReadWriter) LocationRecordArray {
	return &onDiskLocationRecordArray{
		recordFile: recordFile,
		lock:       sync.Mutex{},
	}
}

func (lra *onDiskLocationRecordArray) Get(index int) LocationRecord {
	lra.lock.Lock()
	defer lra.lock.Unlock()

	offset := int64(index * 60)
	record := make([]byte, 60)
	lra.recordFile.ReadAt(record, offset)
	return deserializeLocationRecord(record)
}

func (lra *onDiskLocationRecordArray) Put(index int, locationRecord LocationRecord) {
	lra.lock.Lock()
	defer lra.lock.Unlock()

	offset := int64(index * 60)
	record := serializeLocationRecord(locationRecord)
	lra.recordFile.WriteAt(record, offset)
}
