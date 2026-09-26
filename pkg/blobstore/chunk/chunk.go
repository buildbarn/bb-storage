package chunk

import (
	"bytes"
	"context"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed. The uncompressed data of a chunk
// is always available, as creators of chunks are required to decompress
// and verify the chunk against its digest prior to creation. Chunk
// objects are thread safe but not trivially copyable, refer to them by
// pointer.
type Chunk struct {
	data []byte

	zstdPool       zstd.Pool
	compressedData atomic.Pointer[[]byte]
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
func (c *Chunk) GetBytes() []byte {
	return c.data
}

// GetBytesCompressed gets the underlying bytes of the chunk in ZSTD
// compressed format.
func (c *Chunk) GetBytesCompressed(ctx context.Context) ([]byte, error) {
	if compressedData := c.compressedData.Load(); compressedData != nil {
		return *compressedData, nil
	}
	return c.compress(ctx)
}

func (c *Chunk) compress(ctx context.Context) ([]byte, error) {
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
	compressedData := buf.Bytes()
	c.compressedData.Store(&compressedData)
	return compressedData, nil
}

// NewChunk creates a chunk from an uncompressed byte slice. Ownership
// of the provided slice is transferred to the chunk, callers must not
// modify or reuse it afterwards.
func NewChunk(zstdPool zstd.Pool, data []byte) *Chunk {
	c := &Chunk{data: data, zstdPool: zstdPool}
	return c
}

// NewChunkWithCompressedData creates a chunk that has data in both its
// compressed and uncompressed form. It is the responsibility of the
// caller to make sure that this chunk is valid. Ownership of the
// provided slices are transferred to the chunk, callers must not modify
// or reuse them afterwards.
func NewChunkWithCompressedData(data, compressedData []byte) *Chunk {
	c := &Chunk{data: data}
	c.compressedData.Store(&compressedData)
	return c
}

var (
	emptyData     = []byte{}
	emptyZstdData = []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x01, 0x00, 0x00}
)

// EmptyChunk is a special chunk which the REv2 API requires to be
// present in all stores.
var EmptyChunk = func() *Chunk {
	c := &Chunk{data: emptyData}
	c.compressedData.Store(&emptyZstdData)
	return c
}()
