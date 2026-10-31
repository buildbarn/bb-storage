package chunk

import (
	"context"
	"io"

	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
)

// digestsReader is an io.Reader that stitches together the contents of
// a blob based on an ordered slice of chunk digests.
type digestsReader struct {
	ctx              context.Context
	chunkBytesReader reader.Reader[[]byte]
	chunkDigests     []digest.Digest

	// Index of the next chunk to be fetched.
	nextChunkIndex int
	// Chunks fetched ahead of the consumer. Only the first entry may
	// have been consumed partially.
	buffered       [][]byte
	bufferedOffset int
}

// NewReaderFromDigests creates an io.Reader that yields the
// concatenated contents of the chunks identified by the provided
// digests.
func NewReaderFromDigests(ctx context.Context, chunkDigests []digest.Digest, chunkBytesReader reader.Reader[[]byte]) io.Reader {
	return &digestsReader{
		ctx:              ctx,
		chunkBytesReader: chunkBytesReader,
		chunkDigests:     chunkDigests,
	}
}

// prefetch fetches all chunks that are needed to fill a buffer of the
// given size, so that the subsequent copy never has to block on a
// single chunk fetch. Chunks are fetched concurrently.
func (r *digestsReader) prefetch(size int) error {
	// Determine how many chunks are required to fill the buffer.
	available := int64(0)
	for i, data := range r.buffered {
		if i == 0 {
			available += int64(len(data) - r.bufferedOffset)
		} else {
			available += int64(len(data))
		}
	}
	end := r.nextChunkIndex
	for available < int64(size) && end < len(r.chunkDigests) {
		available += r.chunkDigests[end].GetSizeBytes()
		end++
	}
	if end == r.nextChunkIndex {
		return nil
	}

	// Fetch the chunks concurrently. If any of them fails, the entire
	// batch is discarded and the error is reported to the caller
	// without yielding any of the data that was fetched successfully.
	chunkData := make([][]byte, end-r.nextChunkIndex)
	base := r.nextChunkIndex
	group, ctx := errgroup.WithContext(r.ctx)
	for i := base; i < end; i++ {
		group.Go(func() error {
			data, err := r.chunkBytesReader.Read(ctx, r.chunkDigests[i])
			if err != nil {
				return util.StatusWrapf(err, "Failed to fetch chunk at index %d", i)
			}
			chunkData[i-base] = data
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		// Discard the batch, so that a retry starts from a clean slate.
		return err
	}
	r.buffered = append(r.buffered, chunkData...)
	r.nextChunkIndex = end
	return nil
}

func (r *digestsReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.prefetch(len(p)); err != nil {
		return 0, err
	}

	// Copy as much data as available from the buffered chunks into p.
	// As prefetch fetches all chunks required this will only be short
	// if the end of the blob is reached.
	n := 0
	for n < len(p) && len(r.buffered) > 0 {
		c := copy(p[n:], r.buffered[0][r.bufferedOffset:])
		n += c
		r.bufferedOffset += c
		if r.bufferedOffset >= len(r.buffered[0]) {
			r.buffered[0] = nil
			r.buffered = r.buffered[1:]
			r.bufferedOffset = 0
		}
	}

	if len(r.buffered) == 0 && r.nextChunkIndex >= len(r.chunkDigests) {
		return n, io.EOF
	}
	return n, nil
}
