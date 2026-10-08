package reader

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/lossymap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cachingReader[T any] struct {
	Reader[T]
	clock clock.Clock
	cache lossymap.Map[digest.Digest, T, time.Time]
	lock  sync.Mutex
}

// NewCachingReader decorates a Reader with a cache, so that values
// that were read recently are returned from memory instead of being
// read from the underlying Reader again.
func NewCachingReader[T any](base Reader[T], clock clock.Clock, cache lossymap.Map[digest.Digest, T, time.Time]) Reader[T] {
	return &cachingReader[T]{
		Reader: base,
		clock:  clock,
		cache:  cache,
	}
}

func (r *cachingReader[T]) Read(ctx context.Context, d digest.Digest) (T, error) {
	r.lock.Lock()
	value, err := r.cache.Get(d, r.clock.Now())
	r.lock.Unlock()
	if err == nil {
		return value, nil
	} else if status.Code(err) != codes.NotFound {
		var zero T
		return zero, err
	}
	value, err = r.Reader.Read(ctx, d)
	if err != nil {
		var zero T
		return zero, err
	}
	r.lock.Lock()
	err = r.cache.Put(d, value, r.clock.Now())
	r.lock.Unlock()
	if err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}
