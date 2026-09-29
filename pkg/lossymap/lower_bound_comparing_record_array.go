package lossymap

type lowerBoundComparingRecordArray[TKey comparable, TValue any] struct {
	base            RecordArray[TKey, TValue, struct{}]
	valueComparator ValueComparator[TValue]
}

// NewLowerBoundComparingRecordArray creates a decorator for RecordArray
// that suppresses any entries whose value are lower than a provided
// value. This can, for example, be used to suppress entries that are
// expired or refer to stale data.
func NewLowerBoundComparingRecordArray[TKey comparable, TValue any](
	base RecordArray[TKey, TValue, struct{}],
	valueComparator ValueComparator[TValue],
) RecordArray[TKey, TValue, TValue] {
	return &lowerBoundComparingRecordArray[TKey, TValue]{
		base:            base,
		valueComparator: valueComparator,
	}
}

func (ra *lowerBoundComparingRecordArray[TKey, TValue]) Get(index uint64, lowestValidValue TValue) (Record[TKey, TValue], error) {
	record, err := ra.base.Get(index, struct{}{})
	if err != nil {
		return Record[TKey, TValue]{}, err
	}
	if ra.valueComparator(&record.Value, &lowestValidValue) < 0 {
		return Record[TKey, TValue]{}, ErrRecordInvalidOrExpired
	}
	return record, nil
}

func (ra *lowerBoundComparingRecordArray[TKey, TValue]) Put(index uint64, record Record[TKey, TValue], lowestValidValue TValue) error {
	if ra.valueComparator(&record.Value, &lowestValidValue) < 0 {
		return nil
	}
	return ra.base.Put(index, record, struct{}{})
}
