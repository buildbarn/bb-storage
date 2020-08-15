package local_test

import (
	"context"
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLocalBlobAccessAllocationPattern(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digestLocationMap := mock.NewMockDigestLocationMap(ctrl)
	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	var blocks []*mock.MockBlock
	for i := 0; i < 8; i++ {
		block := mock.NewMockBlock(ctrl)
		blocks = append(blocks, block)
		blockAllocator.EXPECT().NewBlock().Return(block, nil)
	}
	blobAccess, err := local.NewLocalBlobAccess(
		digestLocationMap,
		blockAllocator,
		errorLogger,
		digest.KeyWithoutInstance,
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
	errorLogger := mock.NewMockErrorLogger(ctrl)

	block2 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block2, nil)
	block3 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block3, nil)
	blobAccess, err := local.NewLocalBlobAccess(
		digestLocationMap,
		blockAllocator,
		errorLogger,
		digest.KeyWithoutInstance,
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
		block2.EXPECT().Get(digest1, int64(0), int64(5), gomock.Any()).Return(buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))),
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
	missing, err := blobAccess.FindMissing(ctx, digest1.ToSingletonSet())
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
		block3.EXPECT().Get(digest2, int64(0), int64(5), gomock.Any()).Return(buffer.NewValidatedBufferFromByteSlice([]byte("World"))),
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

func TestLocalBlobAccessDataIntegrityError(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digestLocationMap := mock.NewMockDigestLocationMap(ctrl)
	blockAllocator := mock.NewMockBlockAllocator(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)

	block2 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block2, nil)
	block3 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block3, nil)
	block4 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block4, nil)
	blobAccess, err := local.NewLocalBlobAccess(
		digestLocationMap,
		blockAllocator,
		errorLogger,
		digest.KeyWithoutInstance,
		"cas",
		/* sectorSizeBytes = */ 1,
		/* blockSectorCount = */ 5,
		/* oldBlocksCount = */ 1,
		/* currentBlocksCount = */ 1,
		/* newBlocksCount = */ 2)
	require.NoError(t, err)

	// Read a blob that corresponds with "Hello" from block 3. Let
	// block 3 return the contents "xyzzy" instead. This should
	// cause LocalBlobAccess to report that blocks 2 to 3 are
	// discarded due to a data integrity error.
	helloDigest := digest.MustNewDigest("hello", "8b1a9953c4611296a827abf8c47804d7", 5)
	helloCompactDigest := local.NewCompactDigest("8b1a9953c4611296a827abf8c47804d7-5")
	digestLocationMap.EXPECT().Get(helloCompactDigest, &local.LocationValidator{
		OldestValidBlockID: 2,
		NewestValidBlockID: 4,
	}).Return(local.Location{
		BlockID:     3,
		OffsetBytes: 0,
		SizeBytes:   5,
	}, nil)
	block3.EXPECT().Get(
		helloDigest,
		int64(0),
		int64(5),
		gomock.Any(),
	).DoAndReturn(func(digest digest.Digest, offsetBytes int64, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
		return buffer.NewCASBufferFromByteSlice(helloDigest, []byte("xyzzy"), buffer.BackendProvided(dataIntegrityCallback))
	})
	invalidationWait := make(chan struct{})
	errorLogger.EXPECT().Log(status.Error(codes.Internal, "Discarded blocks 2 to 3 due to a data integrity error")).DoAndReturn(func(err error) {
		close(invalidationWait)
	})

	_, err = blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
	require.Equal(t, status.Error(codes.Internal, "Buffer has checksum 1271ed5ef305aadabc605b1609e24c52, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)
	<-invalidationWait

	// Subsequent reads should no longer send requests to the
	// digest-location map for blocks 2 and 3.
	digestLocationMap.EXPECT().Get(helloCompactDigest, &local.LocationValidator{
		OldestValidBlockID: 4,
		NewestValidBlockID: 4,
	}).Return(local.Location{}, status.Error(codes.NotFound, "Blob not found"))

	_, err = blobAccess.Get(ctx, helloDigest).ToByteSlice(10)
	require.Equal(t, status.Error(codes.NotFound, "Blob not found"), err)

	// Subsequent writes should also no longer consider block 3 to
	// contain any useful space. It should immediately get rotated
	// to make space for block 5. The blob should be placed in the
	// first valid "new" block; block 4.
	block5 := mock.NewMockBlock(ctrl)
	blockAllocator.EXPECT().NewBlock().Return(block5, nil)
	block4.EXPECT().Put(int64(0), gomock.Any()).DoAndReturn(func(offsetBytes int64, b buffer.Buffer) error {
		data, err := b.ToByteSlice(10)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
		return nil
	})
	digestLocationMap.EXPECT().Put(
		helloCompactDigest,
		&local.LocationValidator{
			OldestValidBlockID: 4,
			NewestValidBlockID: 5,
		},
		local.Location{
			BlockID:     4,
			OffsetBytes: 0,
			SizeBytes:   5,
		})

	require.NoError(t, blobAccess.Put(ctx, helloDigest, buffer.NewValidatedBufferFromByteSlice([]byte("Hello"))))
}

// TODO: Make unit testing coverage more complete.
