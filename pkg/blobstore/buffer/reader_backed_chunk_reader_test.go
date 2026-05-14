package buffer_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/stretchr/testify/require"
)

func TestReaderBackedChunkReaderReusesBuffer(t *testing.T) {
	const (
		chunkSize = 16
		blobSize  = int64(chunkSize * 4)
	)
	data := make([]byte, blobSize)
	for i := range data {
		data[i] = byte(i)
	}

	b := buffer.NewValidatedBufferFromReaderAt(readAtCloser{bytes.NewReader(data)}, blobSize)
	r := b.ToChunkReader(0, chunkSize)
	defer r.Close()

	first, err := r.Read()
	require.NoError(t, err)
	require.Equal(t, chunkSize, len(first))
	firstAddr := &first[0]

	for i := 0; i < 3; i++ {
		next, err := r.Read()
		require.NoError(t, err)
		require.Equal(t, chunkSize, len(next))
		require.Same(t, firstAddr, &next[0],
			"successive chunks must share backing storage")
	}

	_, err = r.Read()
	require.Equal(t, io.EOF, err)
}

// TestReaderBackedChunkReaderClosePoolsBuffer retries because
// sync.Pool may drop entries between calls (e.g. on GC).
func TestReaderBackedChunkReaderClosePoolsBuffer(t *testing.T) {
	const chunkSize = 4096
	data := make([]byte, chunkSize)

	var observedReuse bool
	for attempt := 0; attempt < 100 && !observedReuse; attempt++ {
		b1 := buffer.NewValidatedBufferFromReaderAt(readAtCloser{bytes.NewReader(data)}, int64(chunkSize))
		r1 := b1.ToChunkReader(0, chunkSize)
		chunk, err := r1.Read()
		require.NoError(t, err)
		addr1 := &chunk[0]
		_, err = r1.Read()
		require.Equal(t, io.EOF, err)
		r1.Close()

		b2 := buffer.NewValidatedBufferFromReaderAt(readAtCloser{bytes.NewReader(data)}, int64(chunkSize))
		r2 := b2.ToChunkReader(0, chunkSize)
		chunk2, err := r2.Read()
		require.NoError(t, err)
		if &chunk2[0] == addr1 {
			observedReuse = true
		}
		r2.Close()
	}
	require.True(t, observedReuse,
		"expected sync.Pool to return a previously-released buffer at least once across 100 attempts")
}
