package reader

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/ttlcache"
)

type cachingReader[T any] struct {
	Reader[T]
	cache *ttlcache.TTLCache[digest.Digest, T]
}

// NewCachingReader decorates a Reader with a TTLCache, so that values
// that were read recently are returned from memory instead of being
// read from the underlying Reader again.
func NewCachingReader[T any](base Reader[T], cache *ttlcache.TTLCache[digest.Digest, T]) Reader[T] {
	return &cachingReader[T]{
		Reader: base,
		cache:  cache,
	}
}

func (r *cachingReader[T]) Read(ctx context.Context, d digest.Digest) (T, error) {
	if value, ok := r.cache.Get(d); ok {
		return value, nil
	}
	value, err := r.Reader.Read(ctx, d)
	if err != nil {
		var zero T
		return zero, err
	}
	r.cache.Put(d, value)
	return value, nil
}
