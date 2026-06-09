package blobstore

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	digest_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/singleflight"
)

type cachedItem[V any] struct {
	value      V
	expiration time.Time
}

// TTLCache provides a generic, concurrency-safe cache with TTL and
// eviction for string keys. Keys are limited to strings as this is what
// singleflight is limited to.
type TTLCache[V any] struct {
	clock         clock.Clock
	evictionSet   eviction.Set[string]
	maxItems      int
	cacheDuration time.Duration

	lock  sync.Mutex
	items map[string]cachedItem[V]

	flightGroup singleflight.Group
}

// NewTTLCache instantiates a reusable TTLCache for any key-value pair.
func NewTTLCache[V any](clock clock.Clock, evictionSet eviction.Set[string], maxItems int, cacheDuration time.Duration) *TTLCache[V] {
	return &TTLCache[V]{
		clock:         clock,
		evictionSet:   evictionSet,
		maxItems:      maxItems,
		cacheDuration: cacheDuration,
		items:         make(map[string]cachedItem[V]),
	}
}

// NewTTLCacheFromConfiguration wraps NewTTLCache with the parameters
// specified in a configuration message.
func NewTTLCacheFromConfiguration[V any](configuration *digest_pb.ExistenceCacheConfiguration, clock clock.Clock, name string) (*TTLCache[V], error) {
	cacheDuration := configuration.CacheDuration
	if err := cacheDuration.CheckValid(); err != nil {
		return nil, util.StatusWrap(err, "Invalid cache duration")
	}
	evictionSet, err := eviction.NewSetFromConfiguration[string](configuration.CacheReplacementPolicy)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create eviction set")
	}
	return NewTTLCache[V](
		clock,
		eviction.NewMetricsSet(evictionSet, name),
		int(configuration.CacheSize),
		cacheDuration.AsDuration(),
	), nil
}

// GetOrSet retrieves the value from the cache, or executes the fetcher
// exactly once for concurrent callers of the same key.
//
// Note on cancellation: To prevent a single caller from canceling the
// shared underlying fetch, the execution uses a detached context.
func (c *TTLCache[V]) GetOrSet(ctx context.Context, key string, fetch func(context.Context, string) (V, error)) (V, error) {
	// Fast path, get directly from the cache.
	if val, ok := c.Get(key); ok {
		return val, nil
	}

	// Slow path, call the fetcher in a deduplicated manner such that
	// only one call to fetch is performed for the same key.

	// Detach the cancellation function of the context to allow
	// deduplicated calls to proceed even if the original caller
	// cancels. As a limitation of singleflight we are unable to cancel
	// the call in the situation where all callers have cancelled their
	// contexts but that situation should be rare.
	detachedCtx := context.WithoutCancel(ctx)

	// The flightGroup deduplicates all calls for the same key.
	ch := c.flightGroup.DoChan(key, func() (interface{}, error) {
		// Check the cache inside the singleflight scope. Protects us
		// from performing an extra fetch in case there was a put in
		// between our previous check and our flight taking of.
		if val, ok := c.Get(key); ok {
			return val, nil
		}

		// Execute the fetch
		val, err := fetch(detachedCtx, key)
		if err != nil {
			return nil, err
		}

		c.Put(key, val)
		return val, nil
	})

	select {
	case <-ctx.Done():
		// This specific caller has cancelled, return immediately.
		var zero V
		return zero, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			var zero V
			return zero, res.Err
		}
		return res.Val.(V), nil
	}
}

// Get retrieves an item if it exists and hasn't expired.
func (c *TTLCache[V]) Get(key string) (V, bool) {
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
func (c *TTLCache[V]) Put(key string, value V) {
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
