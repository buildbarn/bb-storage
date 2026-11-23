package cas

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"

	"golang.org/x/sync/semaphore"
)

type fetchedChunk[T any] struct {
	done      chan struct{}
	value     T
	err       error
	holdsUnit bool
}

// FetchChunksInOrder fetches chunks and passes them to consume() in
// order. While a chunk is consumed, the next chunks are fetched in the
// background.
//
// The chunk that is consumed next is always fetched. Any chunk after
// it is only fetched ahead if a unit of the semaphore can be acquired
// without waiting. The semaphore thus limits the memory that all calls
// use together for reading ahead, while a call never waits for other
// calls. The unit is released once the chunk is consumed.
func FetchChunksInOrder[T any](ctx context.Context, chunkDigests []digest.Digest, fetch func(context.Context, digest.Digest) (T, error), readAheadSemaphore *semaphore.Weighted, consume func(T) error) error {
	ctx, cancel := context.WithCancel(ctx)
	fetched := make([]*fetchedChunk[T], 0, len(chunkDigests))
	consumed := 0
	defer func() {
		cancel()
		for _, c := range fetched[consumed:] {
			<-c.done
			if c.holdsUnit {
				readAheadSemaphore.Release(1)
			}
		}
	}()

	startFetch := func(holdsUnit bool) {
		c := &fetchedChunk[T]{
			done:      make(chan struct{}),
			holdsUnit: holdsUnit,
		}
		chunkDigest := chunkDigests[len(fetched)]
		fetched = append(fetched, c)
		go func() {
			c.value, c.err = fetch(ctx, chunkDigest)
			close(c.done)
		}()
	}

	for consumed < len(chunkDigests) {
		if len(fetched) == consumed {
			startFetch(false)
		}
		for len(fetched) < len(chunkDigests) && readAheadSemaphore.TryAcquire(1) {
			startFetch(true)
		}

		c := fetched[consumed]
		<-c.done
		err := c.err
		if err == nil {
			err = consume(c.value)
		}
		fetched[consumed] = nil
		consumed++
		if c.holdsUnit {
			readAheadSemaphore.Release(1)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
