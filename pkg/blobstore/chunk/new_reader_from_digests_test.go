package chunk_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeChunkBytesReader is an in-memory implementation of
// reader.Reader[[]byte] that can be instructed to fail for specific
// chunks.
type fakeChunkBytesReader struct {
	lock sync.Mutex
	data map[digest.Digest][]byte
	errs map[digest.Digest]error
}

func (r *fakeChunkBytesReader) Read(ctx context.Context, d digest.Digest) ([]byte, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err, ok := r.errs[d]; ok {
		return nil, err
	}
	data, ok := r.data[d]
	if !ok {
		return nil, status.Error(codes.NotFound, "Chunk not found")
	}
	return data, nil
}

func newFakeChunkBytesReader(t *testing.T, chunkData ...string) (*fakeChunkBytesReader, []digest.Digest) {
	digestFunction := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)
	r := &fakeChunkBytesReader{
		data: map[digest.Digest][]byte{},
		errs: map[digest.Digest]error{},
	}
	digests := make([]digest.Digest, 0, len(chunkData))
	for _, data := range chunkData {
		c := chunk.NewChunk([]byte(data))
		d := c.ComputeDigest(digestFunction)
		r.data[d] = c.GetBytes()
		digests = append(digests, d)
	}
	return r, digests
}

// TestReaderFromDigestsConcatenatesChunks verifies that the reader
// yields the concatenation of all chunks, independently of how the
// caller sizes its read buffer.
func TestReaderFromDigestsConcatenatesChunks(t *testing.T) {
	chunks := []string{"Hello, ", "world! ", "This is ", "a blob."}
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, chunks...)

	for _, bufferSize := range []int{1, 3, 7, 64} {
		r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
		var data []byte
		buf := make([]byte, bufferSize)
		for {
			n, err := r.Read(buf)
			data = append(data, buf[:n]...)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}
		require.Equal(t, "Hello, world! This is a blob.", string(data))
	}
}

// TestReaderFromDigestsFillsBuffer verifies that the reader does not
// return short reads before the end of the blob, even though the data
// is spread across multiple chunks. Callers such as io.ReadFull() rely
// on this.
func TestReaderFromDigestsFillsBuffer(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "def", "ghi")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	buf := make([]byte, 9)
	n, err := io.ReadFull(r, buf)
	require.NoError(t, err)
	require.Equal(t, 9, n)
	require.Equal(t, "abcdefghi", string(buf))
}

// TestReaderFromDigestsZeroLengthRead verifies that a read of zero
// bytes does not consume any data.
func TestReaderFromDigestsZeroLengthRead(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "def")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	n, err := r.Read(nil)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "abcdef", string(data))
}

// TestReaderFromDigestsMissingChunk verifies that a read that cannot be
// completed yields an error without returning any data from the chunks
// it did manage to fetch.
func TestReaderFromDigestsMissingChunk(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "def", "ghi")
	chunkBytesReader.errs[chunkDigests[2]] = status.Error(codes.NotFound, "Chunk not found")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	buf := make([]byte, 9)
	n, err := r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestReaderFromDigestsMissingChunkAfterPartialRead verifies that an
// error is reported once the reader reaches the chunk that is missing.
func TestReaderFromDigestsMissingChunkAfterPartialRead(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "def", "ghi")
	chunkBytesReader.errs[chunkDigests[2]] = status.Error(codes.NotFound, "Chunk not found")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	buf := make([]byte, 6)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "abcdef", string(buf))

	n, err = r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, codes.NotFound, status.Code(err))
}

// TestReaderFromDigestsErrorIsRetried verifies that a failed prefetch
// does not consume any data, so that a subsequent read starts over from
// the same chunk.
func TestReaderFromDigestsErrorIsRetried(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "def")
	chunkBytesReader.errs[chunkDigests[1]] = status.Error(codes.NotFound, "Chunk not found")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	buf := make([]byte, 6)
	n, err := r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, codes.NotFound, status.Code(err))

	// Make the chunk available again. The failed read did not consume
	// any data, so the retry must yield the blob from the start.
	delete(chunkBytesReader.errs, chunkDigests[1])
	n, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, "abcdef", string(buf))
}

// TestReaderFromDigestsEmpty verifies that a reader without chunks
// immediately reports the end of the blob.
func TestReaderFromDigestsEmpty(t *testing.T) {
	chunkBytesReader, _ := newFakeChunkBytesReader(t)

	r := chunk.NewReaderFromDigests(context.Background(), nil, chunkBytesReader)
	buf := make([]byte, 1)
	n, err := r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
}

// TestReaderFromDigestsSingleChunk verifies that a single chunk is
// yielded correctly. GetReader() produces such readers when a read
// starts inside the final chunk of a mapping.
func TestReaderFromDigestsSingleChunk(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "abc", string(data))
}

// TestReaderFromDigestsDuplicateChunks verifies that a mapping that
// refers to the same chunk multiple times yields that chunk's data
// repeatedly. Content defined chunking produces such mappings for
// blobs that contain repeated data.
func TestReaderFromDigestsDuplicateChunks(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "abc", "def")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	require.Equal(t, "abcabcdef", string(data))
}

// TestReaderFromDigestsBufferExceedsBlob verifies that a read buffer
// larger than the blob yields all data and end-of-file in a single
// call, and that subsequent reads keep reporting end-of-file.
func TestReaderFromDigestsBufferExceedsBlob(t *testing.T) {
	chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, "abc", "def")

	r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
	buf := make([]byte, 100)
	n, err := r.Read(buf)
	require.Equal(t, 6, n)
	require.Equal(t, io.EOF, err)
	require.Equal(t, "abcdef", string(buf[:n]))

	n, err = r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
}

// FuzzReaderFromDigests verifies that the reader yields the
// concatenation of all chunks for arbitrary chunk sizes and read
// buffer sizes, and that it never returns a short read unless it also
// reports the end of the blob.
func FuzzReaderFromDigests(f *testing.F) {
	f.Add([]byte{1, 2, 3}, uint16(1))
	f.Add([]byte{255, 1}, uint16(64))
	f.Add([]byte{7, 7, 7, 7}, uint16(3))
	f.Add([]byte{1, 0, 2}, uint16(2))

	f.Fuzz(func(t *testing.T, sizes []byte, bufferSize uint16) {
		// Zero length chunks are rejected by
		// NewMappingFromDigests(), so discard them from the input.
		// A zero sized read buffer would never make progress.
		chunkSizes := make([]byte, 0, len(sizes))
		for _, size := range sizes {
			if size != 0 {
				chunkSizes = append(chunkSizes, size)
			}
		}
		if len(chunkSizes) == 0 || bufferSize == 0 {
			return
		}

		// Give every chunk a distinct content, so that duplicate
		// digests only occur when the fuzzer asks for them.
		chunkData := make([]string, 0, len(chunkSizes))
		expected := make([]byte, 0, len(chunkSizes)*255)
		for i, chunkSize := range chunkSizes {
			data := strings.Repeat(string(rune('A'+i%26)), int(chunkSize))
			chunkData = append(chunkData, data)
			expected = append(expected, data...)
		}
		chunkBytesReader, chunkDigests := newFakeChunkBytesReader(t, chunkData...)

		r := chunk.NewReaderFromDigests(context.Background(), chunkDigests, chunkBytesReader)
		buf := make([]byte, bufferSize)
		var actual []byte
		for {
			n, err := r.Read(buf)
			actual = append(actual, buf[:n]...)
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			// A read that does not report end-of-file must fill
			// the buffer completely.
			require.Equal(t, len(buf), n)
		}
		require.Equal(t, expected, actual)
	})
}
