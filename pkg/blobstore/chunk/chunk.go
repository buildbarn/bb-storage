package chunk

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"

	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed. Chunk objects are thread safe but
// not trivially copyable refer to them by pointer.
type Chunk struct {
	zstdPool zstd.Pool

	data           atomic.Pointer[[]byte]
	compressedData atomic.Pointer[[]byte]
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
func (c *Chunk) GetBytes(ctx context.Context) ([]byte, error) {
	if data := c.data.Load(); data != nil {
		return *data, nil
	}
	return c.decompress(ctx)
}

func (c *Chunk) decompress(ctx context.Context) ([]byte, error) {
	compressedData := c.compressedData.Load()
	if compressedData == nil {
		return nil, status.Error(codes.InvalidArgument, "Chunk does not contain compressed data")
	}

	decoder, err := c.zstdPool.NewDecoder(ctx, bytes.NewReader(*compressedData))
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
	c.data.Store(&data)
	return data, nil
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

	var data []byte
	if p := c.data.Load(); p != nil {
		data = *p
	}
	_, err = encoder.Write(data)
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
	c := &Chunk{zstdPool: zstdPool}
	c.data.Store(&data)
	return c
}

// NewChunkFromCompressedData creates a chunk from a compressed byte
// slice. Ownership of the provided slice is transferred to the chunk,
// callers must not modify or reuse it afterwards.
func NewChunkFromCompressedData(zstdPool zstd.Pool, compressedData []byte) *Chunk {
	c := &Chunk{zstdPool: zstdPool}
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
	c := &Chunk{}
	c.data.Store(&emptyData)
	c.compressedData.Store(&emptyZstdData)
	return c
}()
