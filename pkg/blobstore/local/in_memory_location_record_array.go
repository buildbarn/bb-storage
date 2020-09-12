package local

type inMemoryLocationRecord struct {
	recordKey      LocationRecordKey
	blockReference BlockReference
	offsetBytes    int64
	sizeBytes      int64
}

type inMemoryLocationRecordArray struct {
	records  []inMemoryLocationRecord
	resolver BlockReferenceResolver
}

// NewInMemoryLocationRecordArray creates a LocationRecordArray that
// stores its data in memory. HashingKeyLocationMap relies on being able
// to store a mapping from Keys to a Location in memory or on disk. This
// type implements a non-persistent storage of such a map in memory.
func NewInMemoryLocationRecordArray(size int, resolver BlockReferenceResolver) LocationRecordArray {
	return &inMemoryLocationRecordArray{
		records:  make([]inMemoryLocationRecord, size),
		resolver: resolver,
	}
}

func (lra *inMemoryLocationRecordArray) Get(index int) (LocationRecord, error) {
	record := lra.records[index]
	blockIndex, _, found := lra.resolver.BlockReferenceToBlockIndex(record.blockReference)
	if !found {
		return LocationRecord{}, ErrLocationRecordInvalid
	}
	return LocationRecord{
		RecordKey: record.recordKey,
		Location: Location{
			BlockIndex:  blockIndex,
			OffsetBytes: record.offsetBytes,
			SizeBytes:   record.sizeBytes,
		},
	}, nil
}

func (lra *inMemoryLocationRecordArray) Put(index int, locationRecord LocationRecord) error {
	blockReference, _ := lra.resolver.BlockIndexToBlockReference(locationRecord.Location.BlockIndex)
	lra.records[index] = inMemoryLocationRecord{
		recordKey:      locationRecord.RecordKey,
		blockReference: blockReference,
		offsetBytes:    locationRecord.Location.OffsetBytes,
		sizeBytes:      locationRecord.Location.SizeBytes,
	}
	return nil
}
