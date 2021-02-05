package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBlockAllocatorNewBlock(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockAllocator := local.NewInMemoryBlockAllocator(1024)

	block, location, err := blockAllocator.NewBlock()
	require.NoError(t, err)
	require.Nil(t, location)

	// Write an object into the block.
	require.NoError(t, block.Put(
		123,
		buffer.NewValidatedBufferFromByteSlice([]byte("Hello world"))))

	// Extract it once again, using the right offset and size.
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	data, err := block.Get(
		digest.MustNewDigest("hello", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 456),
		123,
		11,
		dataIntegrityCallback.Call).ToByteSlice(1024)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello world"), data)

	block.Release()
}

func TestInMemoryBlockAllocatorNewBlockAtLocation(t *testing.T) {
	blockAllocator := local.NewInMemoryBlockAllocator(1024)

	// InMemoryBlockAllocator provide no persistency, so
	// NewBlockAtLocation() should simply not function. There is no
	// way to get historical blocks back.
	_, found := blockAllocator.NewBlockAtLocation(nil)
	require.False(t, found)

	_, found = blockAllocator.NewBlockAtLocation(&pb.BlockLocation{
		OffsetBytes: 1024,
		SizeBytes:   1024,
	})
	require.False(t, found)
}
