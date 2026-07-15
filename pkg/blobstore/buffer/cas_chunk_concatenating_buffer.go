package buffer

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// ChunkGetter is a callback used by casChunkedBuffer to lazily fetch
// the buffers of individual chunks. This abstraction prevents circular
// dependencies between the buffer package and the blobstore package.
type ChunkGetter func(ctx context.Context, digest digest.Digest) Buffer

type casChunkConcatenatingBuffer struct {
	ctx          context.Context
	chunkGetter  ChunkGetter
	blobDigest   digest.Digest
	chunkDigests []digest.Digest
	source       Source
}

// NewValidatedCASChunkConcatenatingBuffer creates an optimized buffer
// for an object stored in the Content Addressable Storage composed of a
// validated list of chunks. Because the chunk list is treated as
// correct, validation is limited to validating any underlying chunks
// read allowing for random access reads.
func NewValidatedCASChunkConcatenatingBuffer(ctx context.Context, blobDigest digest.Digest, chunkDigests []digest.Digest, chunkGetter ChunkGetter, source Source) Buffer {
	return &casChunkConcatenatingBuffer{
		ctx:          ctx,
		chunkGetter:  chunkGetter,
		blobDigest:   blobDigest,
		chunkDigests: chunkDigests,
		source:       source,
	}
}

// NewUnvalidatedCASChunkConcatenatingBuffer creates a buffer for a chunk list
// provided by an untrusted source. It falls back to the standard
// validating chunk reader stream to guarantee the overarching blob
// checksum is strictly validated.
func NewUnvalidatedCASChunkConcatenatingBuffer(ctx context.Context, blobDigest digest.Digest, chunkDigests []digest.Digest, chunkGetter ChunkGetter, source Source, maximumMessageSizeBytes int) Buffer {
	reader := &chunkConcatenatingChunkReader{
		ctx:          ctx,
		chunkGetter:  chunkGetter,
		chunkDigests: chunkDigests,
		chunkOffset:  0,
		maxChunkSize: maximumMessageSizeBytes,
	}
	return NewCASBufferFromChunkReader(blobDigest, reader, source)
}

func (b *casChunkConcatenatingBuffer) GetSizeBytes() (int64, error) {
	return b.blobDigest.GetSizeBytes(), nil
}

func (casChunkConcatenatingBuffer) Discard() {}

func (b *casChunkConcatenatingBuffer) IntoWriter(w io.Writer) error {
	for _, d := range b.chunkDigests {
		chunkBuf := b.chunkGetter(b.ctx, d)
		if err := chunkBuf.IntoWriter(w); err != nil {
			return err
		}
	}
	return nil
}

func (b *casChunkConcatenatingBuffer) findChunkOffset(off int64) (index int, chunkOffset int64) {
	var accumulatedSize int64
	for i, d := range b.chunkDigests {
		chunkSize := d.GetSizeBytes()
		if accumulatedSize+chunkSize > off {
			return i, off - accumulatedSize
		}
		accumulatedSize += chunkSize
	}
	return len(b.chunkDigests), 0
}

func (b *casChunkConcatenatingBuffer) ReadAt(p []byte, off int64) (int, error) {
	if err := validateReaderOffset(b.blobDigest.GetSizeBytes(), off); err != nil {
		return 0, err
	}

	index, chunkOffset := b.findChunkOffset(off)
	bytesRead := 0

	for index < len(b.chunkDigests) {
		d := b.chunkDigests[index]

		n, err := b.chunkGetter(b.ctx, d).ReadAt(p[bytesRead:], chunkOffset)
		bytesRead += n
		// Error when reading chunk.
		if err != nil {
			if err != io.EOF {
				return bytesRead, util.StatusWrapf(err, "Error when reading chunk at index %d", index)
			}
			if int64(n) < d.GetSizeBytes()-chunkOffset {
				return bytesRead, status.Errorf(codes.Internal, "Expected buffer to be %d bytes but it was only %d.", d.GetSizeBytes(), n+int(chunkOffset))
			}
		}

		if bytesRead == len(p) {
			return bytesRead, nil
		}
		chunkOffset = 0
		index++
	}

	// return io.EOF if we couldn't fill the buffer
	if bytesRead < len(p) {
		return bytesRead, io.EOF
	}
	return bytesRead, nil
}

func (b *casChunkConcatenatingBuffer) ToProto(m proto.Message, maximumSizeBytes int) (proto.Message, error) {
	return toProtoViaByteSlice(b, m, maximumSizeBytes)
}

func (b *casChunkConcatenatingBuffer) ToByteSlice(maximumSizeBytes int) ([]byte, error) {
	expectedSizeBytes := b.blobDigest.GetSizeBytes()
	if expectedSizeBytes > int64(maximumSizeBytes) {
		return nil, status.Errorf(codes.InvalidArgument, "Buffer is %d bytes in size, while a maximum of %d bytes is permitted.", expectedSizeBytes, maximumSizeBytes)
	}
	data := make([]byte, expectedSizeBytes)
	if expectedSizeBytes > 0 {
		// ReadAt is safe to call here. While both ReadAt and
		// ToByteSlice assumes ownership this buffer.Buffer
		// implementation is stateless.
		n, err := b.ReadAt(data, 0)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if int64(n) != expectedSizeBytes {
			return nil, status.Errorf(codes.Internal, "Buffer is %d bytes in size, while %d bytes were expected.", n, expectedSizeBytes)
		}
	}
	return data, nil
}

func (b *casChunkConcatenatingBuffer) ToChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	if err := validateReaderOffset(b.blobDigest.GetSizeBytes(), off); err != nil {
		return newErrorChunkReader(err)
	}

	index, chunkOffset := b.findChunkOffset(off)

	return &chunkConcatenatingChunkReader{
		ctx:          b.ctx,
		chunkGetter:  b.chunkGetter,
		chunkDigests: b.chunkDigests,
		chunkOffset:  chunkOffset,
		currentIndex: index,
		maxChunkSize: maximumChunkSizeBytes,
	}
}

func (b *casChunkConcatenatingBuffer) ToReader() io.ReadCloser {
	return b.toUnvalidatedReader(0)
}

func (b *casChunkConcatenatingBuffer) toUnvalidatedReader(off int64) io.ReadCloser {
	return newChunkReaderBackedReader(b.toUnvalidatedChunkReader(off, 0))
}

func (b *casChunkConcatenatingBuffer) CloneCopy(maximumSizeBytes int) (Buffer, Buffer) {
	return b, b
}

func (b *casChunkConcatenatingBuffer) CloneStream() (Buffer, Buffer) {
	return newCASClonedBuffer(b, b.blobDigest, b.source).CloneStream()
}

func (b *casChunkConcatenatingBuffer) WithTask(task func() error) Buffer {
	return newCASBufferWithBackgroundTask(b, b.blobDigest, b.source, task)
}

func (b *casChunkConcatenatingBuffer) applyErrorHandler(errorHandler ErrorHandler) (Buffer, bool) {
	return newCASErrorHandlingBuffer(b, errorHandler, b.blobDigest, b.source), false
}

func (b *casChunkConcatenatingBuffer) toUnvalidatedChunkReader(off int64, maximumChunkSizeBytes int) ChunkReader {
	return b.ToChunkReader(off, maximumChunkSizeBytes)
}

// chunkConcatenatingChunkReader fullfills the buffer.ChunkReader
// interface.
type chunkConcatenatingChunkReader struct {
	ctx          context.Context
	chunkGetter  ChunkGetter
	chunkDigests []digest.Digest
	chunkOffset  int64
	maxChunkSize int

	currentIndex  int
	currentReader ChunkReader
	closed        bool
}

func (r *chunkConcatenatingChunkReader) Read() ([]byte, error) {
	if r.closed {
		return nil, status.Error(codes.Internal, "Reader is already closed")
	}
	for {
		if r.currentReader == nil {
			if r.currentIndex >= len(r.chunkDigests) {
				return nil, io.EOF
			}
			currentDigest := r.chunkDigests[r.currentIndex]
			chunkBuf := r.chunkGetter(r.ctx, currentDigest)
			maxChunkSize := r.maxChunkSize
			if maxChunkSize <= 0 {
				maxChunkSize = int(currentDigest.GetSizeBytes())
			}
			r.currentReader = chunkBuf.ToChunkReader(r.chunkOffset, maxChunkSize)
			r.chunkOffset = 0
			r.currentIndex++
		}

		data, err := r.currentReader.Read()
		if len(data) > 0 {
			return data, nil
		}
		if err == io.EOF {
			r.currentReader.Close()
			r.currentReader = nil
			continue
		}
		if err != nil {
			r.currentReader.Close()
			r.currentReader = nil
			return nil, err
		}
	}
}

func (r *chunkConcatenatingChunkReader) Close() {
	r.closed = true
	if r.currentReader != nil {
		r.currentReader.Close()
		r.currentReader = nil
	}
}
