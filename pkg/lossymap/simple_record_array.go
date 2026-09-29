package lossymap

type simpleRecordArray[TKey comparable, TValue any] struct {
	records []Record[TKey, TValue]
}

// NewSimpleRecordArray creates a RecordArray that is backed by a slice
// that resides in memory.
func NewSimpleRecordArray[TKey comparable, TValue any](count int) RecordArray[TKey, TValue, struct{}] {
	return &simpleRecordArray[TKey, TValue]{
		records: make([]Record[TKey, TValue], count),
	}
}

func (ra *simpleRecordArray[TKey, TValue]) Get(index uint64, unused struct{}) (Record[TKey, TValue], error) {
	return ra.records[index], nil
}

func (ra *simpleRecordArray[TKey, TValue]) Put(index uint64, record Record[TKey, TValue], unused struct{}) error {
	ra.records[index] = record
	return nil
}
