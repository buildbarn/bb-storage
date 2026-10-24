package chunk

import (
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed. The uncompressed data of a chunk
// is always available, as creators of chunks are required to decompress
// and verify the chunk against its digest prior to creation. Chunk
// objects are thread safe but not trivially copyable, refer to them by
// pointer.
type Chunk struct {
	data []byte

	encodedData atomic.Pointer[[]byte]
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
func (c *Chunk) GetBytes() []byte {
	return c.data
}

// GetEncodedBytes gets the underlying bytes of the chunk in the given
// encoded format. The IDENTITY compressor yields the uncompressed
// bytes.
func (c *Chunk) GetEncodedBytes(compressor remoteexecution.Compressor_Value) ([]byte, error) {
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return c.data, nil
	case remoteexecution.Compressor_ZSTD:
		if encodedData := c.encodedData.Load(); encodedData != nil {
			return *encodedData, nil
		}
		encodedData := zstd.Encode(c.data)
		c.encodedData.Store(&encodedData)
		return encodedData, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}

// ComputeDigest computes the digest of the underlying chunk.
func (c *Chunk) ComputeDigest(digestFunction digest.Function) digest.Digest {
	data := c.GetBytes()
	generator := digestFunction.NewGenerator(int64(len(data)))
	if _, err := generator.Write(data); err != nil {
		panic("Should not be possible, got error while computing digest of fixed size byte slice: " + err.Error())
	}
	return generator.Sum()
}

// NewChunk creates a chunk from an uncompressed byte slice. Ownership
// of the provided slice is transferred to the chunk, callers must not
// modify or reuse it afterwards.
func NewChunk(data []byte) *Chunk {
	return &Chunk{data: data}
}

// NewChunkFromEncoded creates a chunk from a byte slice in the given
// encoded format. The IDENTITY compressor accepts uncompressed bytes.
// In both cases the decoded data may not exceed
// maximumDecodedSizeBytes. Ownership of the provided slice is
// transferred to the chunk, callers must not modify it afterwards.
func NewChunkFromEncoded(encodedData []byte, maximumDecodedSizeBytes int, compressor remoteexecution.Compressor_Value) (*Chunk, error) {
	if maximumDecodedSizeBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "Maximum decoded size cannot be negative")
	}
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		if len(encodedData) > maximumDecodedSizeBytes {
			return nil, status.Errorf(codes.InvalidArgument, "Chunk data is %d bytes in size, while a maximum of %d bytes is permitted", len(encodedData), maximumDecodedSizeBytes)
		}
		return &Chunk{data: encodedData}, nil
	case remoteexecution.Compressor_ZSTD:
		data, err := zstd.Decode(encodedData, maximumDecodedSizeBytes)
		if err != nil {
			return nil, err
		}

		c := &Chunk{data: data}
		c.encodedData.Store(&encodedData)
		return c, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}
