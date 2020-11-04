package local

import (
	"errors"
)

// ErrLocationRecordInvalid is an error code that may be returned by
// LocationRecordArray.Get() to indicate that the LocationRecord stored
// at a given index is invalid.
//
// Entries are invalid if they have never been set, or when they point
// to a location that is no longer valid. The latter can happen when
// BlockList.PopFront() is called.
//
// This error should never be returned to the user. It should be caught
// by consumers of LocationRecordArray, such as HashingKeyLocationMap.
var ErrLocationRecordInvalid = errors.New("Location record invalid")

// LocationRecord is a key-value pair that contains information on where
// a blob may be found.
type LocationRecord struct {
	RecordKey LocationRecordKey
	Location  Location
}

// LocationRecordArray is equivalent to a []LocationRecord. It is used
// as the backing store by HashingKeyLocationMap. Instead of storing
// data in a slice in memory, an implementation could store this
// information on disk for a persistent data store.
type LocationRecordArray interface {
	Get(index int) (LocationRecord, error)
	Put(index int, locationRecord LocationRecord) error
}
