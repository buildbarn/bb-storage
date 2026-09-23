package chunk

import (
	"bytes"
	"context"
	"io"
	"sync"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed. Chunk objects are thread safe but
// not trivially copyable refer to them by pointer.
type Chunk struct {
	zstdPool zstd.Pool

	mu sync.Mutex

	hasData atomic.Bool
	data    []byte

	hasCompressedData atomic.Bool
	compressedData    []byte
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
func (c *Chunk) GetBytes(ctx context.Context) ([]byte, error) {
	if !c.hasData.Load() {
		return c.decompress(ctx)
	}
	return c.data, nil
}

func (c *Chunk) decompress(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.hasData.Load() {
		// Someone else has decompressed before we could acquire the
		// lock, nothing to do.
		return c.data, nil
	}

	decoder, err := c.zstdPool.NewDecoder(ctx, bytes.NewReader(c.compressedData))
	if err != nil {
		// An error acquiring the decoder, we return the error but do
		// not save the result.
		return nil, err
	}
	defer decoder.Close()

	data, err := io.ReadAll(decoder)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.InvalidArgument, "Could not decompress data")
	}
	c.data = data
	c.hasData.Store(true)
	return data, nil
}

// GetBytesCompressed gets the underlying bytes of the chunk in ZSTD
// compressed format.
func (c *Chunk) GetBytesCompressed(ctx context.Context) ([]byte, error) {
	if !c.hasCompressedData.Load() {
		return c.compress(ctx)
	}
	return c.compressedData, nil
}

func (c *Chunk) compress(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.hasCompressedData.Load() {
		// Someone else has compressed before we could acquire the lock,
		// nothing to do.
		return c.compressedData, nil
	}

	var buf bytes.Buffer
	encoder, err := c.zstdPool.NewEncoder(ctx, &buf)
	if err != nil {
		// An error acquiring the encoder, we return the error but do
		// not save the result.
		return nil, err
	}

	_, err = encoder.Write(c.data)
	closeErr := encoder.Close()

	if err == nil {
		// No error while writing but we might have an error while
		// closing.
		err = closeErr
	}

	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Could not compress data")
	}
	c.compressedData = buf.Bytes()
	c.hasCompressedData.Store(true)

	return c.compressedData, nil
}

// NewChunk creates a chunk from an uncompressed byte slice. Ownership
// of the provided slice is transferred to the chunk, so that callers
// may not modify or reuse it afterwards.
func NewChunk(zstdPool zstd.Pool, data []byte) *Chunk {
	c := &Chunk{
		zstdPool: zstdPool,
		data:     data,
	}
	c.hasData.Store(true)
	return c
}

// NewChunkFromCompressedData creates a chunk from a compressed byte
// slice. Ownership of the provided slice is transferred to the chunk,
// so that callers may not modify or reuse it afterwards.
func NewChunkFromCompressedData(zstdPool zstd.Pool, compressedData []byte) *Chunk {
	c := &Chunk{
		zstdPool:       zstdPool,
		compressedData: compressedData,
	}
	c.hasCompressedData.Store(true)
	return c
}

var emptyZstdFrame = []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x01, 0x00, 0x00}

// EmptyChunk is a special chunk which the REv2 API requires to be
// present in all stores.
var EmptyChunk *Chunk = &Chunk{
	data:           []byte{},
	compressedData: emptyZstdFrame,
}

func init() {
	EmptyChunk.hasData.Store(true)
	EmptyChunk.hasCompressedData.Store(true)
}
