package lossymap

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	hashMapPrometheusMetrics sync.Once

	hashMapGetAttempts = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "lossymap",
			Name:      "hash_map_get_attempts",
			Help:      "Number of attempts it took for Get()",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 6),
		},
		[]string{"name", "outcome"},
	)
	hashMapGetTooManyAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "lossymap",
			Name:      "hash_map_get_too_many_attempts_total",
			Help:      "Number of times Get() took the maximum number of attempts and still did not find the entry, which may indicate the hash table is too small",
		},
		[]string{"name"},
	)

	hashMapPutIterations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "lossymap",
			Name:      "hash_map_put_iterations",
			Help:      "Number of iterations it took for Put()",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		},
		[]string{"name", "outcome"},
	)
	hashMapPutTooManyIterations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "lossymap",
			Name:      "hash_map_put_too_many_iterations_total",
			Help:      "Number of times Put() discarded an entry, because it took the maximum number of iterations, which may indicate the hash table is too small",
		},
		[]string{"name"},
	)
)

// RecordKeyHasher is used by the hash map to determine at which index a
// given record needs to be stored. This callback function needs to
// return a 64-bit hash value.
type RecordKeyHasher[TKey comparable] func(recordKey *RecordKey[TKey]) uint64

// ValueComparator is used by the hash map to sort values by age. In
// case of collisions, the hash map prefers displacing older values.
// This means that newer values are stored at more preferable locations.
type ValueComparator[TValue any] func(a, b *TValue) int

type hashMap[TKey comparable, TValue, TExpirationData any] struct {
	recordArray        RecordArray[TKey, TValue, TExpirationData]
	recordCount        uint64
	recordKeyHasher    RecordKeyHasher[TKey]
	valueComparator    ValueComparator[TValue]
	maximumGetAttempts uint8
	maximumPutAttempts int

	getNotFound        prometheus.Observer
	getFound           prometheus.Observer
	getTooManyAttempts prometheus.Counter

	putInserted          prometheus.Observer
	putUpdated           prometheus.Observer
	putIgnoredOlder      prometheus.Observer
	putTooManyAttempts   prometheus.Observer
	putTooManyIterations prometheus.Counter
}

// NewHashMap creates a lossy map that is backed by a fixed size hash
// table.
//
// This implementation uses a collision resolution scheme that is
// inspired to Robin Hood hashing, which displaces entries based on the
// probe sequence length (PSL). However, this implementation displaces
// them based on the age of values. Older data is thus pushed deeper
// into the hash table. This means that probing of the hash table may
// terminate when encountering entries that are expired.
func NewHashMap[TKey comparable, TValue, TExpirationData any](
	recordArray RecordArray[TKey, TValue, TExpirationData],
	recordKeyHasher RecordKeyHasher[TKey],
	recordCount uint64,
	valueComparator ValueComparator[TValue],
	maximumGetAttempts uint8,
	maximumPutAttempts int,
	name string,
) Map[TKey, TValue, TExpirationData] {
	hashMapPrometheusMetrics.Do(func() {
		prometheus.MustRegister(hashMapGetAttempts)
		prometheus.MustRegister(hashMapGetTooManyAttempts)

		prometheus.MustRegister(hashMapPutIterations)
		prometheus.MustRegister(hashMapPutTooManyIterations)
	})

	return &hashMap[TKey, TValue, TExpirationData]{
		recordArray:        recordArray,
		recordKeyHasher:    recordKeyHasher,
		recordCount:        recordCount,
		valueComparator:    valueComparator,
		maximumGetAttempts: maximumGetAttempts,
		maximumPutAttempts: maximumPutAttempts,

		getNotFound:        hashMapGetAttempts.WithLabelValues(name, "NotFound"),
		getFound:           hashMapGetAttempts.WithLabelValues(name, "Found"),
		getTooManyAttempts: hashMapGetTooManyAttempts.WithLabelValues(name),

		putInserted:          hashMapPutIterations.WithLabelValues(name, "Inserted"),
		putUpdated:           hashMapPutIterations.WithLabelValues(name, "Updated"),
		putIgnoredOlder:      hashMapPutIterations.WithLabelValues(name, "IgnoredOlder"),
		putTooManyAttempts:   hashMapPutIterations.WithLabelValues(name, "TooManyAttempts"),
		putTooManyIterations: hashMapPutTooManyIterations.WithLabelValues(name),
	}
}

func (m *hashMap[TKey, TValue, TExpirationData]) Get(key TKey, expirationData TExpirationData) (TValue, error) {
	recordKey := RecordKey[TKey]{Key: key}
	for {
		index := m.recordKeyHasher(&recordKey) % m.recordCount
		record, err := m.recordArray.Get(index, expirationData)
		if err == ErrRecordInvalidOrExpired {
			m.getNotFound.Observe(float64(recordKey.Attempt + 1))
			var bad TValue
			return bad, status.Error(codes.NotFound, "Record not found")
		} else if err != nil {
			var bad TValue
			return bad, err
		}
		if record.RecordKey == recordKey {
			m.getFound.Observe(float64(recordKey.Attempt + 1))
			return record.Value, nil
		}
		recordKey.Attempt++
		if recordKey.Attempt >= m.maximumGetAttempts {
			m.getTooManyAttempts.Inc()
			var bad TValue
			return bad, status.Error(codes.NotFound, "Record not found")
		}
	}
}

func (m *hashMap[TKey, TValue, TExpirationData]) Put(key TKey, value TValue, expirationData TExpirationData) error {
	record := Record[TKey, TValue]{
		RecordKey: RecordKey[TKey]{Key: key},
		Value:     value,
	}
	for iteration := 1; iteration <= m.maximumPutAttempts; iteration++ {
		index := m.recordKeyHasher(&record.RecordKey) % m.recordCount
		oldRecord, err := m.recordArray.Get(index, expirationData)
		if err == ErrRecordInvalidOrExpired {
			// The existing record may be overwritten directly.
			if err := m.recordArray.Put(index, record, expirationData); err != nil {
				return err
			}
			m.putInserted.Observe(float64(iteration))
			return nil
		} else if err != nil {
			return err
		}
		if oldRecord.RecordKey == record.RecordKey {
			// Only allow overwriting an entry if it points
			// to a newer version of the same key.
			if m.valueComparator(&oldRecord.Value, &record.Value) < 0 {
				if err := m.recordArray.Put(index, record, expirationData); err != nil {
					return err
				}
				m.putUpdated.Observe(float64(iteration))
				return nil
			}
			m.putIgnoredOlder.Observe(float64(iteration))
			return nil
		}
		if m.valueComparator(&oldRecord.Value, &record.Value) < 0 {
			// The existing record should be retained, but
			// it does point to an older value than the
			// record we're trying to insert. Displace the
			// old record.
			if err := m.recordArray.Put(index, record, expirationData); err != nil {
				return err
			}
			record = oldRecord
		}
		record.RecordKey.Attempt++
		if record.RecordKey.Attempt >= m.maximumGetAttempts {
			// No need to generate records that Get() cannot reach.
			m.putTooManyAttempts.Observe(float64(iteration))
			return nil
		}
	}
	m.putTooManyIterations.Inc()
	return nil
}
