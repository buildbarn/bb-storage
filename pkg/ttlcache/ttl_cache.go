package ttlcache

import (
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	digest_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type cachedItem[V any] struct {
	value      V
	expiration time.Time
}

// TTLCache provides a generic, concurrency-safe cache with TTL and
// eviction.
type TTLCache[K comparable, V any] struct {
	clock         clock.Clock
	evictionSet   eviction.Set[K]
	maxItems      int
	cacheDuration time.Duration

	lock  sync.Mutex
	items map[K]cachedItem[V]
}

// NewTTLCache instantiates a reusable TTLCache for any key-value pair.
func NewTTLCache[K comparable, V any](clock clock.Clock, evictionSet eviction.Set[K], maxItems int, cacheDuration time.Duration) *TTLCache[K, V] {
	return &TTLCache[K, V]{
		clock:         clock,
		evictionSet:   evictionSet,
		maxItems:      maxItems,
		cacheDuration: cacheDuration,
		items:         make(map[K]cachedItem[V]),
	}
}

// NewTTLCacheFromConfiguration wraps NewTTLCache with the parameters
// specified in a configuration message.
func NewTTLCacheFromConfiguration[K comparable, V any](configuration *digest_pb.ExistenceCacheConfiguration, clock clock.Clock, name string) (*TTLCache[K, V], error) {
	cacheDuration := configuration.CacheDuration
	if err := cacheDuration.CheckValid(); err != nil {
		return nil, util.StatusWrap(err, "Invalid cache duration")
	}
	evictionSet, err := eviction.NewSetFromConfiguration[K](configuration.CacheReplacementPolicy)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create eviction set")
	}
	return NewTTLCache[K, V](
		clock,
		eviction.NewMetricsSet(evictionSet, name),
		int(configuration.CacheSize),
		cacheDuration.AsDuration(),
	), nil
}

// Get retrieves an item if it exists and hasn't expired.
func (c *TTLCache[K, V]) Get(key K) (V, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if cached, ok := c.items[key]; ok {
		if !c.clock.Now().After(cached.expiration) {
			c.evictionSet.Touch(key)
			return cached.value, true
		}
	}

	var zero V
	return zero, false
}

// Put inserts or updates an item in the cache, handling eviction if at
// capacity.
func (c *TTLCache[K, V]) Put(key K, value V) {
	c.lock.Lock()
	defer c.lock.Unlock()

	expiration := c.clock.Now().Add(c.cacheDuration)

	if _, ok := c.items[key]; ok {
		c.items[key] = cachedItem[V]{value: value, expiration: expiration}
		c.evictionSet.Touch(key)
		return
	}

	if len(c.items) >= c.maxItems {
		delete(c.items, c.evictionSet.Peek())
		c.evictionSet.Remove()
	}

	c.items[key] = cachedItem[V]{value: value, expiration: expiration}
	c.evictionSet.Insert(key)
}
