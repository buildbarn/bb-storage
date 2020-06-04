package local

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/buildbarn/bb-storage/pkg/blockdevice"
)

// FileBackedLocationRecordSize is the size of a single LocationRecord in bytes.
const (
	FileBackedLocationRecordSize = sha256.Size + 4 + 8 + 8 + 8
)

type fileBackedLocationRecordArray struct {
	recordFile blockdevice.ReadWriterAt
}

// NewFileBackedLocationRecordArray creates a persistent LocationRecordArray.
// Works by using a file as an array-like structure, writing serialised
// LocationRecords next to each other and calculating where to Read/Write using
// the length of a serialised record multiplied by the index.
func NewFileBackedLocationRecordArray(recordFile blockdevice.ReadWriterAt) LocationRecordArray {
	return &fileBackedLocationRecordArray{
		recordFile: recordFile,
	}
}

func (lra *fileBackedLocationRecordArray) Get(index int) (LocationRecord, error) {
	var record [FileBackedLocationRecordSize]byte
	if _, err := lra.recordFile.ReadAt(record[:], int64(index)*FileBackedLocationRecordSize); err != nil {
		return LocationRecord{}, err
	}

	// Deserialize the read record into a LocationRecord
	l := LocationRecord{
		Key: LocationRecordKey{
			Attempt: binary.LittleEndian.Uint32(record[32:]),
		},
		Location: Location{
			BlockID:     int(binary.LittleEndian.Uint64(record[36:])),
			OffsetBytes: int64(binary.LittleEndian.Uint64(record[44:])),
			SizeBytes:   int64(binary.LittleEndian.Uint64(record[52:])),
		},
	}
	copy(l.Key.Digest[:], record[:])
	return l, nil
}

func (lra *fileBackedLocationRecordArray) Put(index int, locationRecord LocationRecord) error {
	// Serialise the LocationRecord ready to be written to disk
	var record [FileBackedLocationRecordSize]byte
	copy(record[:], locationRecord.Key.Digest[:])
	binary.LittleEndian.PutUint32(record[32:], locationRecord.Key.Attempt)
	binary.LittleEndian.PutUint64(record[36:], uint64(locationRecord.Location.BlockID))
	binary.LittleEndian.PutUint64(record[44:], uint64(locationRecord.Location.OffsetBytes))
	binary.LittleEndian.PutUint64(record[52:], uint64(locationRecord.Location.SizeBytes))

	if _, err := lra.recordFile.WriteAt(record[:60], int64(index)*FileBackedLocationRecordSize); err != nil {
		return err
	}
	return nil
}
