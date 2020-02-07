package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBlockAllocator(t *testing.T) {
	block, err := local.NewInMemoryBlockAllocator(1024).NewBlock()
	require.NoError(t, err)

	// Write an object into the block.
	require.NoError(t, block.Put(
		123,
		buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))

	// Extract it once again, using the right offset and size.
	data, err := block.Get(
		digest.MustNewDigest("hello", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 456),
		123,
		11).ToByteSlice(1024)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)

	block.Release()
}
