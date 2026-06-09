package buffer_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// benchmarkChunkStorage is a mocked blobstore.BlobAccess that reads
// buffer.Buffer objects which have been predeclared. These
// buffer.Buffer objects use the NewCASBufferFromByteSlice
// implementation and can be reused within the context of this test.
// This implementation is used to reduce the overhead of getting items
// from the chunk storage so that our benchmarks can focus on the
// behavior of our buffer implementations.
type benchmarkChunkStorage struct {
	blobstore.BlobAccess
	chunks map[string]buffer.Buffer
}

const (
	chunkSize = 256 << 10
	numChunks = 1000
)

func (m *benchmarkChunkStorage) Get(ctx context.Context, d digest.Digest) buffer.Buffer {
	data, ok := m.chunks[d.GetHashString()]
	if !ok {
		return buffer.NewBufferFromError(status.Errorf(codes.NotFound, "chunk not found"))
	}
	return data
}

func (m *benchmarkChunkStorage) Put(ctx context.Context, d digest.Digest, buf buffer.Buffer) error {
	data, err := buf.ToByteSlice(int(d.GetSizeBytes()))
	if err != nil {
		return err
	}
	m.chunks[d.GetHashString()] = buffer.NewCASBufferFromByteSlice(d, data, buffer.UserProvided)
	return nil
}

func makeRandomData(tb testing.TB, size int, seed int64) []byte {
	tb.Helper()
	data := make([]byte, size)
	r := rand.New(rand.NewSource(seed))
	_, err := r.Read(data)
	require.NoError(tb, err)
	return data
}

func setupBenchmarkData(b *testing.B) (digest.Digest, []byte, []digest.Digest, blobstore.BlobAccess) {
	b.Helper()

	totalSize := chunkSize * numChunks

	data := makeRandomData(b, totalSize, 0)

	var chunkStorage benchmarkChunkStorage
	chunkStorage.chunks = make(map[string]buffer.Buffer, numChunks)

	ctx := context.Background()

	digestFunction := digest.MustNewFunction("benchmark", remoteexecution.DigestFunction_SHA256)

	hash := sha256.Sum256(data)
	blobDigest, _ := digestFunction.NewDigest(hex.EncodeToString(hash[:]), int64(len(data)))

	var chunkDigests []digest.Digest
	for i := 0; i < numChunks; i++ {
		chunkData := data[i*chunkSize : (i+1)*chunkSize]
		chunkHash := sha256.Sum256(chunkData)
		d, _ := digestFunction.NewDigest(hex.EncodeToString(chunkHash[:]), int64(len(chunkData)))

		chunkDigests = append(chunkDigests, d)
		err := chunkStorage.Put(ctx, d, buffer.NewValidatedBufferFromByteSlice(chunkData))
		if err != nil {
			b.Fatalf("Failed to put chunk: %v", err)
		}
	}

	return blobDigest, data, chunkDigests, &chunkStorage
}

type bufferFactory func() buffer.Buffer

func runBufferBenchmarks(b *testing.B, dataSize int64, factory bufferFactory) {
	// Read through the entire buffer via the io.ReadCloser interface.
	b.Run("StreamRead", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := factory()

			reader := buf.ToReader()
			_, err := io.Copy(io.Discard, reader)
			if err != nil {
				b.Fatalf("ReadAll failed: %v", err)
			}

			reader.Close()
		}
	})

	// Read through the entire buffer in chunks up to 1MiB at a time.
	b.Run("ChunkRead_", func(b *testing.B) {
		// 1MiB typical bytestream write chunk.
		const maxChunkSize = 1 << 20

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := factory()

			reader := buf.ToChunkReader(0, maxChunkSize)
			for {
				_, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatalf("ChunkReader failed: %v", err)
				}
			}
			reader.Close()
		}
	})

	// Read a random 4096 byte slice of the buffer.
	b.Run("ReadRand__", func(b *testing.B) {
		p := make([]byte, 4096)
		// Max offset guarantees we stay strictly within bounds.
		maxOffset := dataSize - int64(len(p))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf := factory()

			// Multiply by a large prime to pseudo-randomly scatter the
			// reads across the address space without invoking
			// rand.Int()
			offset := int64(i*999983) % maxOffset

			_, err := buf.ReadAt(p, int64(offset))
			if err != nil && err != io.EOF {
				b.Fatalf("ReadAt failed at offset %d: %v", offset, err)
			}
		}
	})
}

func BenchmarkBuffers(b *testing.B) {
	blobDigest, rawData, chunkDigests, chunkStorage := setupBenchmarkData(b)
	ctx := context.Background()

	// A buffer backed by an in memory byte slice, represents an ideal
	// case.
	b.Run("ByteSlice__", func(b *testing.B) {
		runBufferBenchmarks(b, int64(len(rawData)), func() buffer.Buffer {
			return buffer.NewValidatedBufferFromByteSlice(rawData)
		})
	})

	// ChunkConcatenating buffer where individual chunks gets validated
	// but not the concatenated chunk.
	chunkGetter := chunkStorage.Get
	b.Run("ChunkConcat", func(b *testing.B) {
		runBufferBenchmarks(b, int64(len(rawData)), func() buffer.Buffer {
			return buffer.NewValidatedCASChunkConcatenatingBuffer(ctx, blobDigest, chunkDigests, chunkGetter, buffer.UserProvided)
		})
	})
}
