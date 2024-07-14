package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestVolatileBlockList(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	blockList := local.NewVolatileBlockList(blockAllocator)

	// In the initial state, the BlockList does not contain any
	// blocks. This means that no BlockReferences should be
	// convertible to block indices.
	_, _, found := blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        0,
		BlocksFromLast: 0,
	})
	require.False(t, found)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 0,
	})
	require.False(t, found)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 1,
	})
	require.False(t, found)

	// Attempt to add a new block to the BlockList. Let the initial
	// attempt fail. Errors should be propagated.
	blockAllocator.EXPECT().NewBlock().Return(nil, nil, status.Error(codes.Internal, "No blocks available"))
	require.Equal(
		t,
		status.Error(codes.Internal, "No blocks available"),
		blockList.PushBack())

	block1 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block1, nil, nil)
	require.NoError(t, blockList.PushBack())

	// With the new block added, we should now have entered epoch 1.
	// BlockReferences for epoch 1 should now be accepted. Because
	// there is only one block, the only valid value for
	// BlocksFromLast is zero, resolving to block index 0.
	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        0,
		BlocksFromLast: 0,
	})
	require.False(t, found)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 1,
	})
	require.False(t, found)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 0,
	})
	require.False(t, found)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 1,
	})
	require.False(t, found)

	blockIndex, _, found := blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 0,
	})
	require.True(t, found)
	require.Equal(t, 0, blockIndex)

	// Conversely, index 0 should be convertible to a BlockReference.
	blockReference, _ := blockList.BlockIndexToBlockReference(0)
	require.Equal(t, local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 0,
	}, blockReference)

	// The block should be empty initially.
	block1.EXPECT().HasSpace(int64(160)).Return(true)
	require.True(t, blockList.HasSpace(0, 160))
	block1.EXPECT().HasSpace(int64(161)).Return(false)
	require.False(t, blockList.HasSpace(0, 161))

	// Attempt to write some data into the block. Let one of the
	// writes fail, while another one succeeds. Even for failed
	// writes, the resulting space is wasted.
	block1.EXPECT().Put(int64(5)).Return(func(b buffer.Buffer) local.BlockPutFinalizer {
		b.Discard()
		return func() (int64, error) { return 0, status.Error(codes.Internal, "Disk on fire") }
	})
	_, err := blockList.Put(0, 5)(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))()
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), err)

	block1.EXPECT().Put(int64(5)).Return(func(b buffer.Buffer) local.BlockPutFinalizer {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return func() (int64, error) { return 16, nil }
	})
	offsetBytes, err := blockList.Put(0, 5)(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))()
	require.NoError(t, err)
	require.Equal(t, int64(16), offsetBytes)

	// The amount of available space should have been reduced now.
	block1.EXPECT().HasSpace(int64(128)).Return(true)
	require.True(t, blockList.HasSpace(0, 128))
	block1.EXPECT().HasSpace(int64(129)).Return(false)
	require.False(t, blockList.HasSpace(0, 129))

	// Add a second block to the BlockList.
	block2 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block2, nil, nil)
	require.NoError(t, blockList.PushBack())

	// Ensure that calls to BlockReferenceToBlockIndex() and
	// BlockIndexToBlockReference() return the right results.
	blockIndex, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 0,
	})
	require.True(t, found)
	require.Equal(t, 0, blockIndex)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 1,
	})
	require.False(t, found)

	blockIndex, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 0,
	})
	require.True(t, found)
	require.Equal(t, 1, blockIndex)

	blockIndex, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 1,
	})
	require.True(t, found)
	require.Equal(t, 0, blockIndex)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 2,
	})
	require.False(t, found)

	blockReference, _ = blockList.BlockIndexToBlockReference(0)
	require.Equal(t, local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 1,
	}, blockReference)

	blockReference, _ = blockList.BlockIndexToBlockReference(1)
	require.Equal(t, local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 0,
	}, blockReference)

	// Ensure HasSpace() calls are directed against the right block.
	block1.EXPECT().HasSpace(int64(128)).Return(true)
	require.True(t, blockList.HasSpace(0, 128))
	block1.EXPECT().HasSpace(int64(129)).Return(false)
	require.False(t, blockList.HasSpace(0, 129))
	block2.EXPECT().HasSpace(int64(160)).Return(true)
	require.True(t, blockList.HasSpace(1, 160))
	block2.EXPECT().HasSpace(int64(161)).Return(false)
	require.False(t, blockList.HasSpace(1, 161))

	// Pop the first block off the BlockList. This should cause the
	// first block to be released.
	block1.EXPECT().Release()
	blockList.PopFront()

	// BlockReferenceToBlockIndex() should no longer allow resolving
	// the first block. References to the second block should now
	// return block index zero. All the block indices are
	// decremented by one now.
	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 0,
	})
	require.False(t, found)

	_, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 1,
	})
	require.False(t, found)

	blockIndex, _, found = blockList.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 0,
	})
	require.True(t, found)
	require.Equal(t, 0, blockIndex)

	blockReference, _ = blockList.BlockIndexToBlockReference(0)
	require.Equal(t, local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 0,
	}, blockReference)

	// Ensure HasSpace() forwards calls to the second block now.
	block2.EXPECT().HasSpace(int64(160)).Return(true)
	require.True(t, blockList.HasSpace(0, 160))
	block2.EXPECT().HasSpace(int64(161)).Return(false)
	require.False(t, blockList.HasSpace(0, 161))
}
