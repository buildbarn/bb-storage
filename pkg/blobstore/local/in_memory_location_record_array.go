package local

import (
	"github.com/buildbarn/bb-storage/pkg/lossymap"
)

type inMemoryLocationRecord struct {
	recordKey      LocationRecordKey
	blockReference BlockReference
	offsetBytes    int64
	sizeBytes      int64
}

type inMemoryLocationRecordArray struct {
	records []inMemoryLocationRecord
}

// NewInMemoryLocationRecordArray creates a LocationRecordArray that
// stores its data in memory. lossymap.HashMap relies on being able to
// store a mapping from Keys to a Location in memory or on disk. This
// type implements a non-persistent storage of such a map in memory.
func NewInMemoryLocationRecordArray(size uint64) LocationRecordArray {
	return &inMemoryLocationRecordArray{
		records: make([]inMemoryLocationRecord, size),
	}
}

func (lra *inMemoryLocationRecordArray) Get(index uint64, resolver BlockReferenceResolver) (LocationRecord, error) {
	record := lra.records[index]
	blockIndex, _, found := resolver.BlockReferenceToBlockIndex(record.blockReference)
	if !found {
		return LocationRecord{}, lossymap.ErrRecordInvalidOrExpired
	}
	return LocationRecord{
		RecordKey: record.recordKey,
		Value: Location{
			BlockIndex:  blockIndex,
			OffsetBytes: record.offsetBytes,
			SizeBytes:   record.sizeBytes,
		},
	}, nil
}

func (lra *inMemoryLocationRecordArray) Put(index uint64, locationRecord LocationRecord, resolver BlockReferenceResolver) error {
	blockReference, _ := resolver.BlockIndexToBlockReference(locationRecord.Value.BlockIndex)
	lra.records[index] = inMemoryLocationRecord{
		recordKey:      locationRecord.RecordKey,
		blockReference: blockReference,
		offsetBytes:    locationRecord.Value.OffsetBytes,
		sizeBytes:      locationRecord.Value.SizeBytes,
	}
	return nil
}
