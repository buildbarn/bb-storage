package lossymap

import (
	"time"

	"github.com/buildbarn/bb-storage/pkg/eviction"
	digest_pb "github.com/buildbarn/bb-storage/pkg/proto/configuration/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachedItem[V any] struct {
	value      V
	expiration time.Time
}

// simpleMap provides a generic cache with TTL and eviction. It is a
// concrete implementation of Map, where the expiration data is the
// current time at which the lookup or insert takes place. Entries may
// be dropped at any point in time, either because they expired or
// because the cache reached its maximum size.
type simpleMap[K comparable, V any] struct {
	evictionSet   eviction.Set[K]
	maxItems      int
	cacheDuration time.Duration

	items map[K]cachedItem[V]
}

// NewSimpleMap creates a generic, bounded, concurrency-safe Map with
// TTL and eviction. The supplied eviction.Set is used to determine
// which items to evict when the Map is full.
func NewSimpleMap[K comparable, V any](evictionSet eviction.Set[K], maxItems int, cacheDuration time.Duration) Map[K, V, time.Time] {
	return &simpleMap[K, V]{
		evictionSet:   evictionSet,
		maxItems:      maxItems,
		cacheDuration: cacheDuration,
		items:         make(map[K]cachedItem[V]),
	}
}

// NewSimpleMapFromConfiguration wraps NewSimpleMap with the parameters
// specified in a configuration message.
func NewSimpleMapFromConfiguration[K comparable, V any](configuration *digest_pb.ExistenceCacheConfiguration, name string) (Map[K, V, time.Time], error) {
	cacheDuration := configuration.CacheDuration
	if err := cacheDuration.CheckValid(); err != nil {
		return nil, util.StatusWrap(err, "Invalid cache duration")
	}
	evictionSet, err := eviction.NewSetFromConfiguration[K](configuration.CacheReplacementPolicy)
	if err != nil {
		return nil, util.StatusWrap(err, "Failed to create eviction set")
	}
	return NewSimpleMap[K, V](
		eviction.NewMetricsSet(evictionSet, name),
		int(configuration.CacheSize),
		cacheDuration.AsDuration(),
	), nil
}

func (c *simpleMap[K, V]) Get(key K, now time.Time) (V, error) {
	if cached, ok := c.items[key]; ok && !now.After(cached.expiration) {
		c.evictionSet.Touch(key)
		return cached.value, nil
	}

	var zero V
	return zero, status.Error(codes.NotFound, "Key not found")
}

func (c *simpleMap[K, V]) Put(key K, value V, now time.Time) error {
	expiration := now.Add(c.cacheDuration)

	if _, ok := c.items[key]; ok {
		c.items[key] = cachedItem[V]{value: value, expiration: expiration}
		c.evictionSet.Touch(key)
		return nil
	}

	if len(c.items) >= c.maxItems {
		delete(c.items, c.evictionSet.Peek())
		c.evictionSet.Remove()
	}

	c.items[key] = cachedItem[V]{value: value, expiration: expiration}
	c.evictionSet.Insert(key)
	return nil
}
