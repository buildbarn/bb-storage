package local

import (
	"sync"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	hashingDigestLocationMapPrometheusMetrics sync.Once

	hashingDigestLocationMapGetNotFound = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_get_not_found",
			Help:      "Number of attempts it took for Get() to determine an entry was not found",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 6),
		})
	hashingDigestLocationMapGetFound = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_get_found",
			Help:      "Number of attempts it took for Get() to determine an entry was found",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 6),
		})
	hashingDigestLocationMapGetTooManyAttempts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_get_too_many_attempts_total",
			Help:      "Number of times Get() took the maximum number of attempts and still did not find the entry, which may indicate the hash table is too small",
		})

	hashingDigestLocationMapPutIgnoreInvalid = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_ignore_invalid_total",
			Help:      "Number of times Put() was called with an invalid location",
		})
	hashingDigestLocationMapPutSet = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_set",
			Help:      "Number of iterations it took for Put() to write an entry to an unused slot",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		})
	hashingDigestLocationMapPutUpdate = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_update",
			Help:      "Number of iterations it took for Put() to overwrite an existing entry",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		})
	hashingDigestLocationMapPutIgnoreOlder = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_ignore_older",
			Help:      "Number of iterations it took for Put() to determine the entry was older than the one that exists",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		})
	hashingDigestLocationMapPutTooManyAttempts = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_too_many_attempts",
			Help:      "Number of times Put() discarded an entry, because it would be placed in a location not reachable by Get(), which may indicate the hash table is too small",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 8),
		})
	hashingDigestLocationMapPutTooManyIterations = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "hashing_digest_location_map_put_too_many_iterations_total",
			Help:      "Number of times Put() discarded an entry, because it took the maximum number of iterations, which may indicate the hash table is too small",
		})
)

type hashingDigestLocationMap struct {
	recordArray        LocationRecordArray
	recordsCount       int
	hashInitialization uint64
	maximumGetAttempts uint32
	maximumPutAttempts int
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
func NewHashingDigestLocationMap(recordArray LocationRecordArray, recordsCount int, hashInitialization uint64, maximumGetAttempts uint32, maximumPutAttempts int) DigestLocationMap {
	hashingDigestLocationMapPrometheusMetrics.Do(func() {
		prometheus.MustRegister(hashingDigestLocationMapGetNotFound)
		prometheus.MustRegister(hashingDigestLocationMapGetFound)
		prometheus.MustRegister(hashingDigestLocationMapGetTooManyAttempts)

		prometheus.MustRegister(hashingDigestLocationMapPutIgnoreInvalid)
		prometheus.MustRegister(hashingDigestLocationMapPutSet)
		prometheus.MustRegister(hashingDigestLocationMapPutUpdate)
		prometheus.MustRegister(hashingDigestLocationMapPutIgnoreOlder)
		prometheus.MustRegister(hashingDigestLocationMapPutTooManyAttempts)
		prometheus.MustRegister(hashingDigestLocationMapPutTooManyIterations)
	})

	return &hashingDigestLocationMap{
		recordArray:        recordArray,
		recordsCount:       recordsCount,
		hashInitialization: hashInitialization,
		maximumGetAttempts: maximumGetAttempts,
		maximumPutAttempts: maximumPutAttempts,
	}
}

func (dlm *hashingDigestLocationMap) getSlot(k *LocationRecordKey) int {
	return int(k.Hash(dlm.hashInitialization) % uint64(dlm.recordsCount))
}

func (dlm *hashingDigestLocationMap) Get(digest digest.Digest, validator *LocationValidator) (Location, error) {
	key := NewLocationRecordKey(digest)
	for {
		slot := dlm.getSlot(&key)
		record := dlm.recordArray.Get(slot)
		if !validator.IsValid(record.Location) {
			// Record points to a block that no longer
			// exists. There is no need to continue
			// searching, as everything we find after this
			// point is even older.
			hashingDigestLocationMapGetNotFound.Observe(float64(key.Attempt + 1))
			return Location{}, status.Error(codes.NotFound, "Object not found")
		}
		if record.Key == key {
			hashingDigestLocationMapGetFound.Observe(float64(key.Attempt + 1))
			return record.Location, nil
		}
		key.Attempt++
		if record.Key.Attempt >= dlm.maximumGetAttempts {
			hashingDigestLocationMapGetTooManyAttempts.Inc()
			return Location{}, status.Error(codes.NotFound, "Object not found")
		}
	}
}

func (dlm *hashingDigestLocationMap) Put(digest digest.Digest, validator *LocationValidator, location Location) error {
	if !validator.IsValid(location) {
		hashingDigestLocationMapPutIgnoreInvalid.Inc()
		return nil
	}
	record := LocationRecord{
		Key:      NewLocationRecordKey(digest),
		Location: location,
	}
	for iteration := 1; iteration <= dlm.maximumPutAttempts; iteration++ {
		slot := dlm.getSlot(&record.Key)
		oldRecord := dlm.recordArray.Get(slot)
		if !validator.IsValid(oldRecord.Location) {
			// The existing record may be overwritten directly.
			dlm.recordArray.Put(slot, record)
			hashingDigestLocationMapPutSet.Observe(float64(iteration))
			return nil
		}
		if oldRecord.Key == record.Key {
			// Only allow overwriting an entry if it points
			// to a newer version of the same blob.
			if oldRecord.Location.IsOlder(record.Location) {
				dlm.recordArray.Put(slot, record)
				hashingDigestLocationMapPutUpdate.Observe(float64(iteration))
				return nil
			}
			hashingDigestLocationMapPutIgnoreOlder.Observe(float64(iteration))
			return nil
		}
		if oldRecord.Location.IsOlder(record.Location) {
			// The existing record should be retained, but
			// it does point to older data than the record
			// we're trying to insert. Displace the old
			// record.
			dlm.recordArray.Put(slot, record)
			record = oldRecord
		}
		record.Key.Attempt++
		if record.Key.Attempt >= dlm.maximumGetAttempts {
			// No need to generate records that Get() cannot reach.
			hashingDigestLocationMapPutTooManyAttempts.Observe(float64(iteration))
			return nil
		}
	}
	hashingDigestLocationMapPutTooManyIterations.Inc()
	return nil
}
