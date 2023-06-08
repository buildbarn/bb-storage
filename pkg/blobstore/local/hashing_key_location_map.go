package local

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	hashingKeyLocationMapPrometheusMetrics sync.Once

	hashingKeyLocationMapGetAttempts = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_key_location_map_get_attempts",
			Help:      "Number of attempts it took for Get()",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 6),
		},
		[]string{"storage_type", "outcome"})
	hashingKeyLocationMapGetTooManyAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_key_location_map_get_too_many_attempts_total",
			Help:      "Number of times Get() took the maximum number of attempts and still did not find the entry, which may indicate the hash table is too small",
		},
		[]string{"storage_type"})

	hashingKeyLocationMapPutIterations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_key_location_map_put_iterations",
			Help:      "Number of iterations it took for Put()",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		},
		[]string{"storage_type", "outcome"})
	hashingKeyLocationMapPutTooManyIterations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_key_location_map_put_too_many_iterations_total",
			Help:      "Number of times Put() discarded an entry, because it took the maximum number of iterations, which may indicate the hash table is too small",
		},
		[]string{"storage_type"})
)

type hashingKeyLocationMap struct {
	recordArray        LocationRecordArray
	recordsCount       int
	hashInitialization uint64
	maximumGetAttempts uint32
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

// NewHashingKeyLocationMap creates a KeyLocationMap backed by a hash
// table that uses a strategy similar to Robin Hood hashing to handle
// collisions. By displacing entries for older locations in favour of
// newer locations, older locations are gradually pushed to the
// 'outside' of the hash table.
//
// Data is stored in a LocationRecordArray. Because implementations of
// LocationRecordArray take a BlockReferenceResolver, they can treat
// entries for no longer existent locations as invalid. This makes the
// hash table self-cleaning.
//
// Because the hash table has a limited size (and does not resize),
// there is a risk of the hash collision rate becoming too high. In the
// case of a full hash table, it would even deadlock. To prevent this
// from happening, there is a fixed upper bound on the number of
// iterations Get() and Put() are willing to run. Records will be
// discarded once the upper bound is reached. Though this may sound
// harmful, there is a very high probability that the entry being
// discarded is one of the older ones.
//
// As both the hashing function that is used by LocationRecordKey and
// the slot computation of HashingKeyLocationMap use modulo arithmetic,
// it is recommended to let recordsCount be prime to ensure proper
// distribution of records.
func NewHashingKeyLocationMap(recordArray LocationRecordArray, recordsCount int, hashInitialization uint64, maximumGetAttempts uint32, maximumPutAttempts int, storageType string) KeyLocationMap {
	hashingKeyLocationMapPrometheusMetrics.Do(func() {
		prometheus.MustRegister(hashingKeyLocationMapGetAttempts)
		prometheus.MustRegister(hashingKeyLocationMapGetTooManyAttempts)

		prometheus.MustRegister(hashingKeyLocationMapPutIterations)
		prometheus.MustRegister(hashingKeyLocationMapPutTooManyIterations)
	})

	return &hashingKeyLocationMap{
		recordArray:        recordArray,
		recordsCount:       recordsCount,
		hashInitialization: hashInitialization,
		maximumGetAttempts: maximumGetAttempts,
		maximumPutAttempts: maximumPutAttempts,

		getNotFound:        hashingKeyLocationMapGetAttempts.WithLabelValues(storageType, "NotFound"),
		getFound:           hashingKeyLocationMapGetAttempts.WithLabelValues(storageType, "Found"),
		getTooManyAttempts: hashingKeyLocationMapGetTooManyAttempts.WithLabelValues(storageType),

		putInserted:          hashingKeyLocationMapPutIterations.WithLabelValues(storageType, "Inserted"),
		putUpdated:           hashingKeyLocationMapPutIterations.WithLabelValues(storageType, "Updated"),
		putIgnoredOlder:      hashingKeyLocationMapPutIterations.WithLabelValues(storageType, "IgnoredOlder"),
		putTooManyAttempts:   hashingKeyLocationMapPutIterations.WithLabelValues(storageType, "TooManyAttempts"),
		putTooManyIterations: hashingKeyLocationMapPutTooManyIterations.WithLabelValues(storageType),
	}
}

func (klm *hashingKeyLocationMap) getSlot(k *LocationRecordKey) int {
	return int(k.Hash(klm.hashInitialization) % uint64(klm.recordsCount))
}

func (klm *hashingKeyLocationMap) Get(key Key) (Location, error) {
	recordKey := LocationRecordKey{Key: key}
	for {
		slot := klm.getSlot(&recordKey)
		record, err := klm.recordArray.Get(slot)
		if err == ErrLocationRecordInvalid {
			// Record points to a block that no longer
			// exists. There is no need to continue
			// searching, as everything we find after this
			// point is even older.
			klm.getNotFound.Observe(float64(recordKey.Attempt + 1))
			return Location{}, status.Error(codes.NotFound, "Object not found")
		} else if err != nil {
			return Location{}, err
		}
		if record.RecordKey == recordKey {
			klm.getFound.Observe(float64(recordKey.Attempt + 1))
			return record.Location, nil
		}
		recordKey.Attempt++
		if recordKey.Attempt >= klm.maximumGetAttempts {
			klm.getTooManyAttempts.Inc()
			return Location{}, status.Error(codes.NotFound, "Object not found")
		}
	}
}

func (klm *hashingKeyLocationMap) Put(key Key, location Location) error {
	record := LocationRecord{
		RecordKey: LocationRecordKey{Key: key},
		Location:  location,
	}
	for iteration := 1; iteration <= klm.maximumPutAttempts; iteration++ {
		slot := klm.getSlot(&record.RecordKey)
		oldRecord, err := klm.recordArray.Get(slot)
		if err == ErrLocationRecordInvalid {
			// The existing record may be overwritten directly.
			if err := klm.recordArray.Put(slot, record); err != nil {
				return err
			}
			klm.putInserted.Observe(float64(iteration))
			return nil
		} else if err != nil {
			return err
		}
		if oldRecord.RecordKey == record.RecordKey {
			// Only allow overwriting an entry if it points
			// to a newer version of the same blob.
			if oldRecord.Location.IsOlder(record.Location) {
				if err := klm.recordArray.Put(slot, record); err != nil {
					return err
				}
				klm.putUpdated.Observe(float64(iteration))
				return nil
			}
			klm.putIgnoredOlder.Observe(float64(iteration))
			return nil
		}
		if oldRecord.Location.IsOlder(record.Location) {
			// The existing record should be retained, but
			// it does point to older data than the record
			// we're trying to insert. Displace the old
			// record.
			if err := klm.recordArray.Put(slot, record); err != nil {
				return err
			}
			record = oldRecord
		}
		record.RecordKey.Attempt++
		if record.RecordKey.Attempt >= klm.maximumGetAttempts {
			// No need to generate records that Get() cannot reach.
			klm.putTooManyAttempts.Observe(float64(iteration))
			return nil
		}
	}
	klm.putTooManyIterations.Inc()
	return nil
}
