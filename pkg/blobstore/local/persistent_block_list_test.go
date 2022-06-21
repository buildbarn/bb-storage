package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPersistentBlockListPersistentState(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	blockList, blocksRestored := local.NewPersistentBlockList(blockAllocator, 16, 10, 1, nil)
	require.Equal(t, 0, blocksRestored)

	// The persistent state should match up with how the BlockList
	// was constructed.
	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Empty(t, blockStateList)

	// Attach a couple of new blocks. These should not be part of
	// the persistent state yet, as no epochs were persisted after
	// their creation.
	block1 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block1, &pb.BlockLocation{
		OffsetBytes: 0,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())
	block2 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block2, &pb.BlockLocation{
		OffsetBytes: 160,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())
	block3 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block3, &pb.BlockLocation{
		OffsetBytes: 320,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())

	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Empty(t, blockStateList)

	// Attempt to write a couple of blobs into one of the blocks.
	// This should cause the BlockPutwakeup to trigger. Still, this
	// data hasn't been persisted, so GetPersistentState() should
	// still not return any blocks.
	for i := 0; i < 5; i++ {
		require.True(t, blockList.HasSpace(1, 5))

		block2.EXPECT().Put(int64(i)*16, gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return nil
		})
		offset, err := blockList.Put(1, 5)(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))()
		require.NoError(t, err)
		require.Equal(t, int64(i)*16, offset)
	}

	blockReference, epoch1HashSeed := blockList.BlockIndexToBlockReference(1)
	require.Equal(t, local.BlockReference{
		EpochID:        1,
		BlocksFromLast: 1,
	}, blockReference)

	<-blockList.GetBlockPutWakeup()

	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Empty(t, blockStateList)

	// Successfully synchronize data to disk. This should cause the
	// next call to GetPersistentState() to return all of the
	// blocks.
	blockList.NotifySyncStarting()
	blockList.NotifySyncCompleted()

	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Equal(t, []*pb.BlockState{
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 0,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 0,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 160,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 69,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 320,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 0,
			EpochHashSeeds:   []uint64{epoch1HashSeed},
		},
	}, blockStateList)

	// Write some more blobs into storage. These didn't make it into
	// epoch 1 (which got synced), which is why their
	// BlockReferences should refer to epoch 2. This new epoch
	// shouldn't be returned by GetPersistentState() yet.
	for i := 0; i < 3; i++ {
		require.True(t, blockList.HasSpace(0, 5))

		block1.EXPECT().Put(int64(i)*16, gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
			data, err := b.ToByteSlice(10)
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return nil
		})
		offset, err := blockList.Put(0, 5)(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))()
		require.NoError(t, err)
		require.Equal(t, int64(i)*16, offset)
	}

	blockReference, epoch2HashSeed := blockList.BlockIndexToBlockReference(0)
	require.Equal(t, local.BlockReference{
		EpochID:        2,
		BlocksFromLast: 2,
	}, blockReference)

	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Equal(t, []*pb.BlockState{
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 0,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 0,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 160,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 69,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 320,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 0,
			EpochHashSeeds:   []uint64{epoch1HashSeed},
		},
	}, blockStateList)

	// Sync even more data. This should cause epoch 2 to be reported
	// as part of GetPersistentState() as well. The write offset for
	// the first block should also be increased now.
	blockList.NotifySyncStarting()
	blockList.NotifySyncCompleted()

	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Equal(t, []*pb.BlockState{
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 0,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 37,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 160,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 69,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 320,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 0,
			EpochHashSeeds:   []uint64{epoch1HashSeed, epoch2HashSeed},
		},
	}, blockStateList)

	// Start releasing a block. This should cause the
	// BlockReleaseWakeup to trigger, which is should be used to
	// write out the latest persistent state immediately.
	blockList.PopFront()

	<-blockList.GetBlockReleaseWakeup()

	// Calling NotifyPersistentStateWritten() on its own isn't
	// sufficient to release the underlying block. This is because
	// we have at no point called GetPersistentState() to obtain a
	// copy of the state without the first block in it.
	blockList.NotifyPersistentStateWritten()

	// By calling GetPersistentState() followed by
	// NotifyPersistentStateWritten(), the first block will be
	// released entirely.
	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(1), oldestEpochID)
	require.Equal(t, []*pb.BlockState{
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 160,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 69,
			EpochHashSeeds:   []uint64{},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 320,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 0,
			EpochHashSeeds:   []uint64{epoch1HashSeed, epoch2HashSeed},
		},
	}, blockStateList)

	block1.EXPECT().Release()
	blockList.NotifyPersistentStateWritten()
}

func TestPersistentBlockListPutInterruptedByPopFront(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	blockList, blocksRestored := local.NewPersistentBlockList(blockAllocator, 16, 10, 0, nil)
	require.Equal(t, 0, blocksRestored)

	// Attach a Block to the BlockList in which we're going to write
	// some data.
	block := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block, &pb.BlockLocation{
		OffsetBytes: 0,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())

	putWriter := blockList.Put(0, 5)

	// Release the Block. This should not cause the underlying Block
	// to be released immediately, as there is still a write that is
	// in flight.
	blockList.PopFront()

	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(0), oldestEpochID)
	require.Empty(t, blockStateList)
	blockList.NotifyPersistentStateWritten()

	// Because writing is permitted without holding any locks, there
	// is nothing we can do to prevent the write from occurring. It
	// should still be directed against the underlying Block.
	block.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return nil
	})
	putFinalizer := putWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	// Finalizing the write should immediately cause the Block to be
	// released.
	block.EXPECT().Release()
	_, err := putFinalizer()
	require.Equal(t, status.Error(codes.Internal, "The block to which this blob was written, has already been released"), err)

	// Because the write failed to finalize, the caller isn't going
	// to call BlockIndexToBlockReference(). The current epoch ID
	// should therefore still be zero, even if we forced a
	// synchronization of all data.
	verifyEpochIDStaysAtZero(t, blockList)
}

func TestPersistentBlockListRestorePersistentState(t *testing.T) {
	ctrl := gomock.NewController(t)

	// For every block passed in from previous persistent state, a
	// call to the BlockAllocator should be performed to request a
	// block at that specific offset.
	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	block1 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlockAtLocation(&pb.BlockLocation{
		OffsetBytes: 0,
		SizeBytes:   160,
	}).Return(block1, true)
	block2 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlockAtLocation(&pb.BlockLocation{
		OffsetBytes: 160,
		SizeBytes:   160,
	}).Return(block2, true)

	blockList, blocksRestored := local.NewPersistentBlockList(blockAllocator, 16, 10, 5, []*pb.BlockState{
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 0,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 42,
			EpochHashSeeds:   []uint64{0x7c9dbac171efb2ee},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 160,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 103,
			EpochHashSeeds:   []uint64{0x598d432e782bf169, 0xff2c0b9d38a2b09c},
		},
	})
	require.Equal(t, 2, blocksRestored)

	// GetPersistentState() should return exactly what was passed in
	// when the BlockList was constructed.
	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(5), oldestEpochID)
	require.Equal(t, []*pb.BlockState{
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 0,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 42,
			EpochHashSeeds:   []uint64{0x7c9dbac171efb2ee},
		},
		{
			BlockLocation: &pb.BlockLocation{
				OffsetBytes: 160,
				SizeBytes:   160,
			},
			WriteOffsetBytes: 103,
			EpochHashSeeds:   []uint64{0x598d432e782bf169, 0xff2c0b9d38a2b09c},
		},
	}, blockStateList)

	// We store the write offset in bytes in the Protobuf messages.
	// When allocating data, we should use round these offsets to
	// the next sector.
	require.True(t, blockList.HasSpace(0, 111))
	require.True(t, blockList.HasSpace(0, 112))
	require.False(t, blockList.HasSpace(0, 113))

	require.True(t, blockList.HasSpace(1, 47))
	require.True(t, blockList.HasSpace(1, 48))
	require.False(t, blockList.HasSpace(1, 49))
}

func TestPersistentBlockListPushBackWhenClosedForWriting(t *testing.T) {
	blockList, _ := local.NewPersistentBlockList(nil, 16, 10, 0, nil)

	blockList.CloseForWriting()

	require.Equal(
		t,
		status.Error(codes.Unavailable, "Cannot write object to storage, as storage is shutting down"),
		blockList.PushBack())
}

func TestPersistentBlockListPutWhenClosedForWriting1(t *testing.T) {
	// Writing to a block list consists of calling Put(), which returns a
	// putWriter to be called, which in turn returns a putFinalizer to be
	// called. Make sure error is returned if closed for writing early.
	ctrl := gomock.NewController(t)

	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	blockList, blocksRestored := local.NewPersistentBlockList(blockAllocator, 16, 10, 0, nil)
	require.Equal(t, 0, blocksRestored)

	// Attach a Block to the BlockList in which we're going to write
	// some data.
	block := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block, &pb.BlockLocation{
		OffsetBytes: 0,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())

	// Close for writing even before calling Put.
	blockList.CloseForWriting()

	putWriter := blockList.Put(0, 5)

	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(0), oldestEpochID)
	require.Empty(t, blockStateList)
	blockList.NotifyPersistentStateWritten()

	// Not expecting block.EXPECT().Put().
	putFinalizer := putWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	_, err := putFinalizer()
	require.Equal(
		t,
		status.Error(codes.Unavailable, "Cannot write object to storage, as storage is shutting down"),
		err)

	// When closed for writing, the epoch ID should not change.
	verifyEpochIDStaysAtZero(t, blockList)
}

func TestPersistentBlockListPutWhenClosedForWriting2(t *testing.T) {
	// Writing to a block list consists of calling Put(), which returns a
	// putWriter to be called, which in turn returns a putFinalizer to be
	// called. Make sure error is returned if closed for writing in the last
	// minute.
	ctrl := gomock.NewController(t)

	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	blockList, blocksRestored := local.NewPersistentBlockList(blockAllocator, 16, 10, 0, nil)
	require.Equal(t, 0, blocksRestored)

	// Attach a Block to the BlockList in which we're going to write
	// some data.
	block := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block, &pb.BlockLocation{
		OffsetBytes: 0,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())

	putWriter := blockList.Put(0, 5)

	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(0), oldestEpochID)
	require.Empty(t, blockStateList)
	blockList.NotifyPersistentStateWritten()

	// Close for writing after receiving the putWriter.
	blockList.CloseForWriting()

	// Not expecting block.EXPECT().Put().
	putFinalizer := putWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	_, err := putFinalizer()
	require.Equal(
		t,
		status.Error(codes.Unavailable, "Cannot write object to storage, as storage is shutting down"),
		err)

	// When closed for writing, the epoch ID should not change.
	verifyEpochIDStaysAtZero(t, blockList)
}

func TestPersistentBlockListPutWhenClosedForWriting3(t *testing.T) {
	// Writing to a block list consists of calling Put(), which returns a
	// putWriter to be called, which in turn returns a putFinalizer to be
	// called. Make sure error is returned if closed for writing in the last
	// minute.
	ctrl := gomock.NewController(t)

	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	blockList, blocksRestored := local.NewPersistentBlockList(blockAllocator, 16, 10, 0, nil)
	require.Equal(t, 0, blocksRestored)

	// Attach a Block to the BlockList in which we're going to write
	// some data.
	block := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block, &pb.BlockLocation{
		OffsetBytes: 0,
		SizeBytes:   160,
	}, nil)
	require.NoError(t, blockList.PushBack())

	putWriter := blockList.Put(0, 5)

	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(0), oldestEpochID)
	require.Empty(t, blockStateList)
	blockList.NotifyPersistentStateWritten()

	// Because writing is permitted without holding any locks, there
	// is nothing we can do to prevent the write from occurring. It
	// should still be directed against the underlying Block.
	block.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return nil
	})
	putFinalizer := putWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))

	// Close for writing in the last minute.
	blockList.CloseForWriting()

	_, err := putFinalizer()
	require.Equal(
		t,
		status.Error(codes.Unavailable, "Cannot write object to storage, as storage is shutting down"),
		err)

	// When closed for writing, the epoch ID should not change.
	verifyEpochIDStaysAtZero(t, blockList)
}

func verifyEpochIDStaysAtZero(t *testing.T, blockList *local.PersistentBlockList) {
	// When BlockIndexToBlockReference() has not been called, the current
	// epoch ID should therefore still be zero, even if we forced a
	// synchronization of all data.
	oldestEpochID, blockStateList := blockList.GetPersistentState()
	require.Equal(t, uint32(0), oldestEpochID)
	require.Empty(t, blockStateList)

	blockList.NotifySyncStarting()
	blockList.NotifySyncCompleted()

	oldestEpochID, blockStateList = blockList.GetPersistentState()
	require.Equal(t, uint32(0), oldestEpochID)
	require.Empty(t, blockStateList)
}
