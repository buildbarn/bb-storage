package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBlockAllocator(t *testing.T) {
	block := local.NewInMemoryBlockAllocator(1024).NewBlock()

	// Write an object into the block.
	require.NoError(t, block.Put(
		123,
		buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))

	// Extract it once again, using the right offset and size.
	data, err := block.Get(123, 11).ToByteSlice(1024)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)
}
