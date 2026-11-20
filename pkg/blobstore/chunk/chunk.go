package chunk

import (
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed.
//
// Chunks are either created from data that has already been validated
// against its digest by the creator, or using
// NewChunkWithDelayedValidation(). In the latter case validation is
// performed by the chunk itself, the first time its contents are
// accessed.
//
// Chunk objects are thread safe but not trivially copyable, refer to
// them by pointer.
type Chunk struct {
	// The validated, uncompressed data of the chunk. Not set for
	// chunks whose validation is delayed.
	data []byte

	zstdData atomic.Pointer[[]byte]

	// Only set for chunks whose validation is delayed.
	delayed *delayedValidation
}

// delayedValidation holds the state of a chunk that has not been
// validated against its digest at creation time.
type delayedValidation struct {
	digest digest.Digest

	// The uncompressed data of the chunk, which may not have been
	// validated yet.
	decodeOnce sync.Once
	data       []byte
	decodeErr  error

	validateOnce sync.Once
	validateErr  error
}

// getUnvalidatedBytes returns the uncompressed data of a chunk whose
// validation is delayed, without validating it against the digest.
func (c *Chunk) getUnvalidatedBytes() ([]byte, error) {
	d := c.delayed
	d.decodeOnce.Do(func() {
		sizeBytes := int(d.digest.GetSizeBytes())
		data, err := zstd.Decode(*c.zstdData.Load(), sizeBytes)
		if err != nil {
			d.decodeErr = util.StatusWrapWithCode(err, codes.Internal, "Failed to decode chunk")
		} else if len(data) != sizeBytes {
			d.decodeErr = status.Errorf(codes.Internal, "Chunk is %d bytes in size, while %d bytes were expected", len(data), sizeBytes)
		} else {
			d.data = data
		}
	})
	return d.data, d.decodeErr
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
//
// For chunks created using NewChunkWithDelayedValidation(), this
// decompresses the chunk and validates it against its digest the first
// time it is called. An error is returned if the chunk turns out to be
// malformed.
func (c *Chunk) GetBytes() ([]byte, error) {
	d := c.delayed
	if d == nil {
		return c.data, nil
	}
	data, err := c.getUnvalidatedBytes()
	if err != nil {
		return nil, err
	}
	d.validateOnce.Do(func() {
		generator := d.digest.GetDigestFunction().NewGenerator(int64(len(data)))
		if _, err := generator.Write(data); err != nil {
			panic("Should not be possible, got error while computing digest of fixed size byte slice: " + err.Error())
		}
		if actualDigest := generator.Sum(); actualDigest != d.digest {
			d.validateErr = status.Errorf(codes.Internal, "Digest mismatch, expected %s, got %s", d.digest, actualDigest)
		}
	})
	if d.validateErr != nil {
		return nil, d.validateErr
	}
	return data, nil
}

// GetEncodedBytes gets the underlying bytes of the chunk in the given
// encoded format. The IDENTITY compressor yields the uncompressed
// bytes.
//
// Just like GetBytes(), this validates chunks created using
// NewChunkWithDelayedValidation().
func (c *Chunk) GetEncodedBytes(compressor remoteexecution.Compressor_Value) ([]byte, error) {
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return c.GetBytes()
	case remoteexecution.Compressor_ZSTD:
		data, err := c.GetBytes()
		if err != nil {
			return nil, err
		}
		return c.getZSTDBytes(data), nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}

// GetUnvalidatedEncodedBytes is identical to GetEncodedBytes(), except
// that it does not validate chunks created using
// NewChunkWithDelayedValidation(). If such a chunk is already available
// in the requested format, it is returned as is.
//
// This method MUST only be used to forward the chunk to another party
// that is responsible for validating it, such as a client reading it
// through gRPC. The data that is returned MUST NOT be interpreted or
// placed in storage.
func (c *Chunk) GetUnvalidatedEncodedBytes(compressor remoteexecution.Compressor_Value) ([]byte, error) {
	if c.delayed == nil {
		return c.GetEncodedBytes(compressor)
	}
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return c.getUnvalidatedBytes()
	case remoteexecution.Compressor_ZSTD:
		if encodedData := c.zstdData.Load(); encodedData != nil {
			return *encodedData, nil
		}
		data, err := c.getUnvalidatedBytes()
		if err != nil {
			return nil, err
		}
		return c.getZSTDBytes(data), nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}

// getZSTDBytes returns the ZSTD compressed representation of the
// chunk, compressing the provided uncompressed data if needed.
func (c *Chunk) getZSTDBytes(data []byte) []byte {
	if encodedData := c.zstdData.Load(); encodedData != nil {
		return *encodedData
	}
	encodedData := zstd.Encode(data)
	c.zstdData.Store(&encodedData)
	return encodedData
}

// ComputeDigest computes the digest of the underlying chunk.
//
// Just like GetBytes(), this validates chunks created using
// NewChunkWithDelayedValidation().
func (c *Chunk) ComputeDigest(digestFunction digest.Function) (digest.Digest, error) {
	data, err := c.GetBytes()
	if err != nil {
		return digest.BadDigest, err
	}
	generator := digestFunction.NewGenerator(int64(len(data)))
	if _, err := generator.Write(data); err != nil {
		panic("Should not be possible, got error while computing digest of fixed size byte slice: " + err.Error())
	}
	return generator.Sum(), nil
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
		c.zstdData.Store(&encodedData)
		return c, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}

// NewChunkWithDelayedValidation creates a chunk from a byte slice in
// the given encoded format, without decompressing it and validating it
// against its digest. This is useful for data that was obtained from
// another system, as it is not known up front whether the chunk is
// going to be interpreted, or merely forwarded.
//
// Validation is performed by the chunk itself, the first time its
// contents are requested through GetBytes(), GetEncodedBytes() or
// ComputeDigest(). These methods fail if the chunk turns out not to
// correspond to the digest. Only GetUnvalidatedEncodedBytes() is able
// to return the chunk without validating it.
//
// Ownership of the provided slice is transferred to the chunk, callers
// must not modify it afterwards.
func NewChunkWithDelayedValidation(encodedData []byte, d digest.Digest, compressor remoteexecution.Compressor_Value) (*Chunk, error) {
	c := &Chunk{delayed: &delayedValidation{digest: d}}
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		if sizeBytes := d.GetSizeBytes(); int64(len(encodedData)) != sizeBytes {
			return nil, status.Errorf(codes.InvalidArgument, "Chunk is %d bytes in size, while %d bytes were expected", len(encodedData), sizeBytes)
		}
		c.delayed.data = encodedData
		c.delayed.decodeOnce.Do(func() {})
	case remoteexecution.Compressor_ZSTD:
		c.zstdData.Store(&encodedData)
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
	return c, nil
}
