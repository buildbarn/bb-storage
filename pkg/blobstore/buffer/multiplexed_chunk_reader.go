package buffer

import (
	"sync"
)

type readResult struct {
	data []byte
	err  error
}

type multiplexedChunkReader struct {
	r ChunkReader

	lock             sync.Mutex
	pendingConsumers int
	waitingConsumers []chan readResult
}

// newMultiplexedChunkReader creates a decorator for ChunkReader that
// multiplexes data on the stream to multiple consumers. Calling Read()
// on this stream will hang until all other consumers either call Read()
// or Close().
//
// This multiplexer is used by Buffer.CloneStream(), which can be used
// to implement advanced buffer replication strategies.
func newMultiplexedChunkReader(r ChunkReader, additionalConsumers int) ChunkReader {
	return &multiplexedChunkReader{
		r:                r,
		pendingConsumers: 1 + additionalConsumers,
	}
}

func (r *multiplexedChunkReader) readAndShareWithOthers(currentConsumerContinues int) ([]byte, error) {
	data, err := r.r.Read()
	for _, c := range r.waitingConsumers {
		c <- readResult{data: data, err: err}
	}
	r.pendingConsumers = len(r.waitingConsumers) + currentConsumerContinues
	r.waitingConsumers = r.waitingConsumers[:0]
	return data, err
}

func (r *multiplexedChunkReader) Read() ([]byte, error) {
	r.lock.Lock()
	if r.pendingConsumers <= 0 {
		panic("Multiplexed chunk reader has no pending consumers")
	}
	r.pendingConsumers--

	if r.pendingConsumers == 0 {
		// Last consumer of the stream to call Read(). Call
		// Read() on the underlying ChunkReader and share the
		// data with the rest.
		data, err := r.readAndShareWithOthers(1)
		r.lock.Unlock()
		return data, err
	}

	// At least one more consumer needs to call Read(). Wait for it
	// to share its results.
	c := make(chan readResult, 1)
	r.waitingConsumers = append(r.waitingConsumers, c)
	r.lock.Unlock()
	result := <-c
	return result.data, result.err
}

func (r *multiplexedChunkReader) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.pendingConsumers <= 0 {
		panic("Multiplexed chunk reader has no pending consumers")
	}
	r.pendingConsumers--
	if r.pendingConsumers > 0 {
		return
	}

	if len(r.waitingConsumers) == 0 {
		// All other consumers have left. We're the last to
		// close.
		r.r.Close()
		r.r = nil
	} else {
		// All other consumers are waiting for us to call
		// Read(). Read on their behalf, but opt out from the
		// next iteration.
		r.readAndShareWithOthers(0)
	}
}
