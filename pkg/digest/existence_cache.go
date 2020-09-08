package digest

import (
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
)

// ExistenceCache is a cache of digests, where entries expire once a
// certain duration of time has passed. It is used by
// ExistenceCachingBlobAccess to keep track of which objects may be
// omitted from FindMissing() calls.
//
// It is safe to access ExistenceCache concurrently.
type ExistenceCache struct {
	clock         clock.Clock
	keyFormat     KeyFormat
	cacheSize     int
	cacheDuration time.Duration

	lock           sync.Mutex
	insertionTimes map[string]time.Time
	evictionSet    eviction.Set
}

// NewExistenceCache creates a new ExistenceCache that is empty.
func NewExistenceCache(clock clock.Clock, keyFormat KeyFormat, cacheSize int, cacheDuration time.Duration, evictionSet eviction.Set) *ExistenceCache {
	return &ExistenceCache{
		clock:         clock,
		keyFormat:     keyFormat,
		cacheSize:     cacheSize,
		cacheDuration: cacheDuration,

		insertionTimes: map[string]time.Time{},
		evictionSet:    evictionSet,
	}
}

// RemoveExisting removes digests from a provided set that are present
// in the cache.
func (ec *ExistenceCache) RemoveExisting(digests Set) Set {
	now := ec.clock.Now()
	minimumInsertionTime := now.Add(-ec.cacheDuration)
	missing := NewSetBuilder()
	ec.lock.Lock()
	for _, d := range digests.Items() {
		key := d.GetKey(ec.keyFormat)
		if insertionTime, ok := ec.insertionTimes[key]; ok && !insertionTime.Before(minimumInsertionTime) {
			ec.evictionSet.Touch(key)
		} else {
			missing.Add(d)
		}
	}
	ec.lock.Unlock()
	return missing.Build()
}

// Add digests to the cache. These digests will automatically be removed
// once the duration provided to NewExistenceCache passes.
func (ec *ExistenceCache) Add(digests Set) {
	now := ec.clock.Now()
	ec.lock.Lock()
	for _, d := range digests.Items() {
		// Free up space to insert the digest.
		if len(ec.insertionTimes) >= ec.cacheSize {
			delete(ec.insertionTimes, ec.evictionSet.Peek())
			ec.evictionSet.Remove()
		}

		// Insert new entry or update the existing one.
		key := d.GetKey(ec.keyFormat)
		if insertionTime, ok := ec.insertionTimes[key]; ok {
			if insertionTime.Before(now) {
				ec.insertionTimes[key] = now
			}
		} else {
			ec.insertionTimes[key] = now
			ec.evictionSet.Insert(key)
		}
	}
	ec.lock.Unlock()
}

func (ec *ExistenceCache) Remove(digest Digest) {
	ec.lock.Lock()
	key := digest.GetKey(ec.keyFormat)
	// Simply deleting would mess up the cache size handling above, so
	// we zero the time instead. It will be evicted as usual, but will
	// no longer affect the result of RemoveExisting
	if _, ok := ec.insertionTimes[key]; ok {
		ec.insertionTimes[key] = time.Time{}
	}
	ec.lock.Unlock()
}
