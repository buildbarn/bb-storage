package buffer

import "sync"

// chunkBufferPool caches []byte buffers used by ChunkReader
// implementations that allocate a fixed-size working buffer per
// stream. Buffers are stored as *[]byte to avoid the interface-boxing
// allocation that occurs when storing a non-pointer value in
// sync.Pool.
var chunkBufferPool sync.Pool

func getChunkBuffer(size int) *[]byte {
	if v := chunkBufferPool.Get(); v != nil {
		b := v.(*[]byte)
		if cap(*b) >= size {
			*b = (*b)[:size]
			return b
		}
	}
	b := make([]byte, size)
	return &b
}

func putChunkBuffer(b *[]byte) {
	chunkBufferPool.Put(b)
}
