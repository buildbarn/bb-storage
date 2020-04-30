package local

type inMemoryLocationRecordArray struct {
	records []LocationRecord
}

// NewInMemoryLocationRecordArray creates a LocationRecordArray that
// stores its data in memory. LocalBlobAccess relies on being able to
// store a mapping from digest.Digests to a location in memory or on disk.
// This type implements a non-persistent storage of such a map in
// memory.
func NewInMemoryLocationRecordArray(size int) LocationRecordArray {
	return &inMemoryLocationRecordArray{
		records: make([]LocationRecord, size),
	}
}

func (lra *inMemoryLocationRecordArray) Get(index int) (LocationRecord, error) {
	return lra.records[index], nil
}

func (lra *inMemoryLocationRecordArray) Put(index int, locationRecord LocationRecord) error {
	lra.records[index] = locationRecord
	return nil
}
