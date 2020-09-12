package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPartitioningBlockAllocator(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockDevice := mock.NewMockBlockDevice(ctrl)
	pa := local.NewPartitioningBlockAllocator(blockDevice, blobstore.CASReadBufferFactory, 1, 100, 10)

	// Based on the size of the allocator, it should be possible to
	// create ten blocks.
	var blocks []local.Block
	for i := 0; i < 10; i++ {
		block, err := pa.NewBlock()
		require.NoError(t, err)
		blocks = append(blocks, block)
	}

	// Creating an eleventh block should fail.
	_, err := pa.NewBlock()
	require.Equal(t, err, status.Error(codes.ResourceExhausted, "No unused blocks available"))

	// Blocks should initially be handed out in order of the offset.
	// The third block should thus start at offset 300.
	blockDevice.EXPECT().WriteAt([]byte("Hello"), int64(317)).Return(5, nil)
	require.NoError(t, blocks[3].Put(17, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))

	// Fetch a blob from a block. Don't consume it yet, but do
	// release the block associated with the blob. It should not be
	// possible to reallocate the block as long as the blob hasn't
	// been consumed.
	dataIntegrityCallback := mock.NewMockDataIntegrityCallback(ctrl)
	dataIntegrityCallback.EXPECT().Call(true)
	b := blocks[7].Get(
		digest.MustNewDigest("some-instance", "8b1a9953c4611296a827abf8c47804d7", 5),
		25,
		5,
		dataIntegrityCallback.Call)
	blocks[7].Release()
	_, err = pa.NewBlock()
	require.Equal(t, err, status.Error(codes.ResourceExhausted, "No unused blocks available"))

	// The blob may still be consumed with the block being released.
	// It should have started at offset 700.
	blockDevice.EXPECT().ReadAt(gomock.Any(), int64(725)).DoAndReturn(
		func(p []byte, off int64) (int, error) {
			copy(p, "Hello")
			return 5, nil
		})
	data, err := b.ToByteSlice(100)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello"), data)

	// With the blob being consumed, the underlying block should be
	// released. This means the block can be allocated once again.
	// It should still start at offset 700.
	blocks[7], err = pa.NewBlock()
	require.NoError(t, err)
	blockDevice.EXPECT().WriteAt([]byte("Hello"), int64(741)).Return(5, nil)
	require.NoError(t, blocks[7].Put(41, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))

	// When blocks are reused, they should be allocated according to
	// which one was least recently released. This ensures wear
	// leveling of the storage backend.
	order := []int{2, 8, 4, 9, 3}
	for _, i := range order {
		blocks[i].Release()
	}
	for _, i := range order {
		blocks[i], err = pa.NewBlock()
		require.NoError(t, err)

		blockDevice.EXPECT().WriteAt([]byte("Hello"), int64(100*i+83)).Return(5, nil)
		require.NoError(t, blocks[i].Put(83, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
	}
}
