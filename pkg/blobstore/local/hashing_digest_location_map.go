package local

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	hashingDigestLocationMapPrometheusMetrics sync.Once

	hashingDigestLocationMapGetAttempts = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_get_attempts",
			Help:      "Number of attempts it took for Get()",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 6),
		},
		[]string{"name", "outcome"})
	hashingDigestLocationMapGetTooManyAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_get_too_many_attempts_total",
			Help:      "Number of times Get() took the maximum number of attempts and still did not find the entry, which may indicate the hash table is too small",
		},
		[]string{"name"})

	hashingDigestLocationMapPutIgnoredInvalid = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_ignored_invalid_total",
			Help:      "Number of times Put() was called with an invalid location",
		},
		[]string{"name"})
	hashingDigestLocationMapPutIterations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_iterations",
			Help:      "Number of iterations it took for Put()",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		},
		[]string{"name", "outcome"})
	hashingDigestLocationMapPutTooManyIterations = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_too_many_iterations_total",
			Help:      "Number of times Put() discarded an entry, because it took the maximum number of iterations, which may indicate the hash table is too small",
		},
		[]string{"name"})
)

type hashingDigestLocationMap struct {
	recordArray        LocationRecordArray
	recordsCount       int
	hashInitialization uint64
	maximumGetAttempts uint32
	maximumPutAttempts int

	getNotFound        prometheus.Observer
	getFound           prometheus.Observer
	getTooManyAttempts prometheus.Counter

	putIgnoredInvalid    prometheus.Counter
	putInserted          prometheus.Observer
	putUpdated           prometheus.Observer
	putIgnoredOlder      prometheus.Observer
	putTooManyAttempts   prometheus.Observer
	putTooManyIterations prometheus.Counter
}

// NewHashingDigestLocationMap creates a DigestLocationMap backed by a
// hash table that uses a strategy similar to cuckoo hashing to handle
// collisions. By displacing entries for older locations in favour of
// newer locations, older locations are gradually pushed to the
// 'outside' of the hash table.
//
// Because Get() and Put() take a LocationValidator, they can treat
// entries for no longer existent locations as invalid. This allows the
// hash table to be self-cleaning.
//
// Because the hash table has a limited size (and does not resize),
// there is a risk of the hash collision rate becoming too high. In the
// case of a full hash table, it would even deadlock. To prevent this
// from happening, there is a fixed upper bound on the number of
// iterations Get() and Put() are willing to run. Records will be
// discarded once the upper bound is reached. Though this may sound
// harmful, there is a very high probability that the entry being
// discarded is one of the older ones.
func NewHashingDigestLocationMap(recordArray LocationRecordArray, recordsCount int, hashInitialization uint64, maximumGetAttempts uint32, maximumPutAttempts int, name string) DigestLocationMap {
	hashingDigestLocationMapPrometheusMetrics.Do(func() {
		prometheus.MustRegister(hashingDigestLocationMapGetAttempts)
		prometheus.MustRegister(hashingDigestLocationMapGetTooManyAttempts)

		prometheus.MustRegister(hashingDigestLocationMapPutIgnoredInvalid)
		prometheus.MustRegister(hashingDigestLocationMapPutIterations)
		prometheus.MustRegister(hashingDigestLocationMapPutTooManyIterations)
	})

	return &hashingDigestLocationMap{
		recordArray:        recordArray,
		recordsCount:       recordsCount,
		hashInitialization: hashInitialization,
		maximumGetAttempts: maximumGetAttempts,
		maximumPutAttempts: maximumPutAttempts,

		getNotFound:        hashingDigestLocationMapGetAttempts.WithLabelValues(name, "NotFound"),
		getFound:           hashingDigestLocationMapGetAttempts.WithLabelValues(name, "Found"),
		getTooManyAttempts: hashingDigestLocationMapGetTooManyAttempts.WithLabelValues(name),

		putIgnoredInvalid:    hashingDigestLocationMapPutIgnoredInvalid.WithLabelValues(name),
		putInserted:          hashingDigestLocationMapPutIterations.WithLabelValues(name, "Inserted"),
		putUpdated:           hashingDigestLocationMapPutIterations.WithLabelValues(name, "Updated"),
		putIgnoredOlder:      hashingDigestLocationMapPutIterations.WithLabelValues(name, "IgnoredOlder"),
		putTooManyAttempts:   hashingDigestLocationMapPutIterations.WithLabelValues(name, "TooManyAttempts"),
		putTooManyIterations: hashingDigestLocationMapPutTooManyIterations.WithLabelValues(name),
	}
}

func (dlm *hashingDigestLocationMap) getSlot(k *LocationRecordKey) int {
	return int(k.Hash(dlm.hashInitialization) % uint64(dlm.recordsCount))
}

func (dlm *hashingDigestLocationMap) Get(digest CompactDigest, validator *LocationValidator) (Location, error) {
	key := LocationRecordKey{Digest: digest}
	for {
		slot := dlm.getSlot(&key)
		record, err := dlm.recordArray.Get(slot)
		if err != nil {
			return Location{}, err
		}
		if !validator.IsValid(record.Location) {
			// Record points to a block that no longer
			// exists. There is no need to continue
			// searching, as everything we find after this
			// point is even older.
			dlm.getNotFound.Observe(float64(key.Attempt + 1))
			return Location{}, status.Error(codes.NotFound, "Object not found")
		}
		if record.Key == key {
			dlm.getFound.Observe(float64(key.Attempt + 1))
			return record.Location, nil
		}
		key.Attempt++
		if record.Key.Attempt >= dlm.maximumGetAttempts {
			dlm.getTooManyAttempts.Inc()
			return Location{}, status.Error(codes.NotFound, "Object not found")
		}
	}
}

func (dlm *hashingDigestLocationMap) Put(digest CompactDigest, validator *LocationValidator, location Location) error {
	if !validator.IsValid(location) {
		dlm.putIgnoredInvalid.Inc()
		return nil
	}
	record := LocationRecord{
		Key:      LocationRecordKey{Digest: digest},
		Location: location,
	}
	for iteration := 1; iteration <= dlm.maximumPutAttempts; iteration++ {
		slot := dlm.getSlot(&record.Key)
		oldRecord, err := dlm.recordArray.Get(slot)
		if err != nil {
			return err
		}
		if !validator.IsValid(oldRecord.Location) {
			// The existing record may be overwritten directly.
			if err := dlm.recordArray.Put(slot, record); err != nil {
				return err
			}
			dlm.putInserted.Observe(float64(iteration))
			return nil
		}
		if oldRecord.Key == record.Key {
			// Only allow overwriting an entry if it points
			// to a newer version of the same blob.
			if oldRecord.Location.IsOlder(record.Location) {
				if err := dlm.recordArray.Put(slot, record); err != nil {
					return err
				}
				dlm.putUpdated.Observe(float64(iteration))
				return nil
			}
			dlm.putIgnoredOlder.Observe(float64(iteration))
			return nil
		}
		if oldRecord.Location.IsOlder(record.Location) {
			// The existing record should be retained, but
			// it does point to older data than the record
			// we're trying to insert. Displace the old
			// record.
			if err := dlm.recordArray.Put(slot, record); err != nil {
				return err
			}
			record = oldRecord
		}
		record.Key.Attempt++
		if record.Key.Attempt >= dlm.maximumGetAttempts {
			// No need to generate records that Get() cannot reach.
			dlm.putTooManyAttempts.Observe(float64(iteration))
			return nil
		}
	}
	dlm.putTooManyIterations.Inc()
	return nil
}
