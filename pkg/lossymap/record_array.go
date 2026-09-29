package lossymap

import (
	"errors"
)

// RecordKey combines the key of a map entry with a probe sequence
// length (PSL) counter. These two values need to be hashed to determine
// the index at which the record is stored in the RecordArray.
type RecordKey[TKey any] struct {
	Key     TKey
	Attempt uint8
}

// Record of a key-value pair to store in the RecordArray that backs the
// HashMap.
type Record[TKey, TValue any] struct {
	RecordKey RecordKey[TKey]
	Value     TValue
}

// RecordArray acts as a backing store for HashMap. Typical
// implementations store data in memory or on disk.
type RecordArray[TKey, TValue, TExpirationData any] interface {
	Get(index uint64, expirationData TExpirationData) (Record[TKey, TValue], error)
	Put(index uint64, record Record[TKey, TValue], expirationData TExpirationData) error
}

// ErrRecordInvalidOrExpired may be returned by RecordArray.Get() to
// indicate that a record at a given index is known to be invalid or
// expired. This can be used by HashMap to stop probing before the
// maximum number of attempts is reached.
var ErrRecordInvalidOrExpired = errors.New("record is invalid or expired")
