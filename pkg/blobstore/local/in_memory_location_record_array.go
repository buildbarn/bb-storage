package local

type inMemoryLocationRecordArray struct {
	records []LocationRecord
}

// NewInMemoryLocationRecordArray creates a LocationRecordArray that
// stores its data in memory. LocalBlobAccess relies on being able to
// store a mapping from util.Digests to a location in memory or on disk.
// This type implements a non-persistent storage of such a map in
// memory.
func NewInMemoryLocationRecordArray(size int) LocationRecordArray {
	return &inMemoryLocationRecordArray{
		records: make([]LocationRecord, size),
	}
}

func (lra *inMemoryLocationRecordArray) Get(index int) LocationRecord {
	return lra.records[index]
}

func (lra *inMemoryLocationRecordArray) Put(index int, locationRecord LocationRecord) {
	lra.records[index] = locationRecord
}
