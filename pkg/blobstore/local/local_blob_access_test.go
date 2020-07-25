package local_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestLocalBlobAccessAllocationPattern(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digestLocationMap := mock.NewMockDigestLocationMap(ctrl)
	blockAllocator := mock.NewMockBlockAllocator(ctrl)

	var blocks []*mock.MockBlock
	for i := 0; i < 8; i++ {
		block := mock.NewMockBlock(ctrl)
		blocks = append(blocks, block)
		blockAllocator.EXPECT().NewBlock().Return(block, nil)
	}
	blobAccess, err := local.NewLocalBlobAccess(
		digestLocationMap,
		blockAllocator,
		blobstore.CASStorageType,
		"cas",
		/* sectorSizeBytes = */ 1,
		/* blockSectorCount = */ 16,
		/* oldBlocksCount = */ 2,
		/* currentBlocksCount = */ 4,
		/* newBlocksCount = */ 4)
	require.NoError(t, err)

	// After starting up, there should be a uniform distribution on
	// the "current" blocks and an inverse exponential distribution
	// on the "new" blocks.
	digest := digest.MustNewDigest("example", "3e25960a79dbc69b674cd4ec67a72c62", 11)
	compactDigest := local.NewCompactDigest("3e25960a79dbc69b674cd4ec67a72c62-11")
	allocationAttemptsPerBlock := []int{16, 16, 16, 16, 8, 4, 2, 1}
	for i := 0; i < 10; i++ {
		for j := 0; j < len(blocks); j++ {
			for k := 0; k < allocationAttemptsPerBlock[j]; k++ {
				blocks[j].EXPECT().Put(int64(0), gomock.Any()).Return(nil)
				digestLocationMap.EXPECT().Put(compactDigest, gomock.Any(), local.Location{
					BlockID:     3 + j,
					OffsetBytes: 0,
					SizeBytes:   0,
				})
				require.NoError(t, blobAccess.Put(ctx, digest, buffer.NewValidatedBufferFromByteSlice(nil)))
			}
		}
	}
}

func TestLocalBlobAccessBlockRotationDuringRefreshInOldestBlock(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digestLocationMap := mock.NewMockDigestLocationMap(ctrl)
	blockAllocator := mock.NewMockBlockAllocator(ctrl)

	block2 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block2, nil)
	block3 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block3, nil)
	blobAccess, err := local.NewLocalBlobAccess(
		digestLocationMap,
		blockAllocator,
		blobstore.CASStorageType,
		"cas",
		/* sectorSizeBytes = */ 1,
		/* blockSectorCount = */ 5,
		/* oldBlocksCount = */ 1,
		/* currentBlocksCount = */ 1,
		/* newBlocksCount = */ 1)
	require.NoError(t, err)

	// Store "Hello and "World" to fill up the existing current and
	// new blocks.
	digest1 := digest.MustNewDigest("example", "8b1a9953c4611296a827abf8c47804d7", 5)
	compactDigest1 := local.NewCompactDigest("8b1a9953c4611296a827abf8c47804d7-5")
	block2.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return nil
	})
	digestLocationMap.EXPECT().Put(compactDigest1, gomock.Any(), local.Location{
		BlockID:     2,
		OffsetBytes: 0,
		SizeBytes:   5,
	})
	require.NoError(t, blobAccess.Put(ctx, digest1, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))

	digest2 := digest.MustNewDigest("example", "f5a7924e621e84c9280a9a27e1bcb7f6", 5)
	compactDigest2 := local.NewCompactDigest("f5a7924e621e84c9280a9a27e1bcb7f6-5")
	block3.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("World"), data)
		return nil
	})
	digestLocationMap.EXPECT().Put(compactDigest2, gomock.Any(), local.Location{
		BlockID:     3,
		OffsetBytes: 0,
		SizeBytes:   5,
	})
	require.NoError(t, blobAccess.Put(ctx, digest2, buffer.NewValidatedBufferFromByteSlice([]byte("World"))))

	// Storing "Xyzzy" will cause the block containing "Hello" to
	// become an old block, thereby causing a new block to be
	// allocated.
	block4 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block4, nil)
	digest3 := digest.MustNewDigest("example", "56f2d4d0b97e43f94505299dc45942a1", 5)
	compactDigest3 := local.NewCompactDigest("56f2d4d0b97e43f94505299dc45942a1-5")
	block4.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Xyzzy"), data)
		return nil
	})
	digestLocationMap.EXPECT().Put(compactDigest3, gomock.Any(), local.Location{
		BlockID:     4,
		OffsetBytes: 0,
		SizeBytes:   5,
	})
	require.NoError(t, blobAccess.Put(ctx, digest3, buffer.NewValidatedBufferFromByteSlice([]byte("Xyzzy"))))

	// Calling FindMissing() against "Hello" will cause it to be
	// refreshed. The old block containing "Hello" should be
	// released, but not before Get() is called against it. All of
	// this should cause the next old block to contain "World".
	digestLocationMap.EXPECT().Get(compactDigest1, gomock.Any()).Return(local.Location{
		BlockID:     2,
		OffsetBytes: 0,
		SizeBytes:   5,
	}, nil).Times(2)
	gomock.InOrder(
		block2.EXPECT().Get(digest1, int64(0), int64(5)).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))),
		block2.EXPECT().Release())
	block5 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block5, nil)
	block5.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return nil
	})
	digestLocationMap.EXPECT().Put(compactDigest1, gomock.Any(), local.Location{
		BlockID:     5,
		OffsetBytes: 0,
		SizeBytes:   5,
	})
	missing, err := blobAccess.FindMissing(ctx, digest.NewSetBuilder().Add(digest1).Build())
	require.NoError(t, err)
	require.Equal(t, digest.EmptySet, missing)

	// Calling Get() against "World" will also cause it to be
	// refreshed. Again, the old block containing "World" should be
	// released, but not before Get() is called against it.
	digestLocationMap.EXPECT().Get(compactDigest2, gomock.Any()).Return(local.Location{
		BlockID:     3,
		OffsetBytes: 0,
		SizeBytes:   5,
	}, nil)
	gomock.InOrder(
		block3.EXPECT().Get(digest2, int64(0), int64(5)).Return(buffer.NewValidatedBufferFromByteSlice([]byte("World"))),
		block3.EXPECT().Release())
	block6 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block6, nil)
	block6.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("World"), data)
		return nil
	})
	digestLocationMap.EXPECT().Put(compactDigest2, gomock.Any(), local.Location{
		BlockID:     6,
		OffsetBytes: 0,
		SizeBytes:   5,
	})
	data, err := blobAccess.Get(ctx, digest2).ToByteSlice(10)
	require.NoError(t, err)
	require.Equal(t, []byte("World"), data)
}

// TODO: Make unit testing coverage more complete.
