package local

// LocationRecord is a key-value pair that contains information on where
// a blob may be found.
type LocationRecord struct {
	Key      LocationRecordKey
	Location Location
}

// LocationRecordArray is equivalent to a []LocationRecord. It is used
// as the backing store by HashingDigestLocationMap. Instead of storing
// data in a slice in memory, an implementation could store this
// information on disk for a persistent data store.
type LocationRecordArray interface {
	Get(index int) LocationRecord
	Put(index int, locationRecord LocationRecord)
}
