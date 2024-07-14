package local_test

import (
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/buffer"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestOldCurrentNewLocationBlobMapAllocationPattern(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockList := mock.NewMockBlockList(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
		blockList,
		local.NewImmutableBlockListGrowthPolicy(
			/* currentBlocksCount = */ 4,
			/* newBlocksCount = */ 4),
		errorLogger,
		"cas",
		/* blockSizeBytes = */ 16,
		/* oldBlocksCount = */ 2,
		/* newBlocksCount = */ 4,
		/* initialBlocksCount = */ 10)

	// After starting up, there should be a uniform distribution on
	// the "current" blocks and an inverse exponential distribution
	// on the "new" blocks. No data should be placed in the "old"
	// blocks.
	allocationAttemptsPerBlock := []int{0, 0, 16, 16, 16, 16, 8, 4, 2, 1}
	for i := 0; i < 10; i++ {
		for blockIndex, allocationAttempts := range allocationAttemptsPerBlock {
			for attempt := 0; attempt < allocationAttempts; attempt++ {
				// We should always see a HasSpace()
				// call on block 2, as that is used to
				// determine whether a block rotation
				// needs to be performed.
				blockList.EXPECT().HasSpace(2, int64(5)).Return(true)

				// Ingest the data. Simply assume blocks
				// are infinitely big as part of this test.
				blockList.EXPECT().HasSpace(blockIndex, int64(5)).Return(true)
				blockListPutWriter := mock.NewMockBlockListPutWriter(ctrl)
				blockList.EXPECT().Put(blockIndex, int64(5)).Return(blockListPutWriter.Call)
				blockListPutFinalizer := mock.NewMockBlockListPutFinalizer(ctrl)
				blockListPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
					func(b buffer.Buffer) local.BlockListPutFinalizer {
						data, err := b.ToByteSlice(10)
						require.NoError(t, err)
						require.Equal(t, []byte("Hello"), data)
						return blockListPutFinalizer.Call
					})
				blockListPutFinalizer.EXPECT().Call().Return(int64(123), nil)

				// Perform the Put() operation.
				locationBlobPutWriter, err := locationBlobMap.Put(5)
				require.NoError(t, err)
				location, err := locationBlobPutWriter(buffer.NewValidatedBufferFromByteSlice([]byte("Hello")))()
				require.NoError(t, err)
				require.Equal(t, local.Location{
					BlockIndex:  blockIndex,
					OffsetBytes: 123,
					SizeBytes:   5,
				}, location)
			}
		}
	}
}

func TestOldCurrentNewLocationBlobMapDataCorruption(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockList := mock.NewMockBlockList(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
		blockList,
		local.NewImmutableBlockListGrowthPolicy(
			/* currentBlocksCount = */ 4,
			/* newBlocksCount = */ 4),
		errorLogger,
		"cas",
		/* blockSizeBytes = */ 16,
		/* oldBlocksCount = */ 2,
		/* newBlocksCount = */ 4,
		/* initialBlocksCount = */ 10)

	// Perform a Get() call against block 1. Return a buffer that
	// will trigger a data integrity error, as the digest
	// corresponds with "Hello", not "xyzzy". This should cause the
	// first two blocks to be marked for immediate release.
	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	blockList.EXPECT().Get(2, helloDigest, int64(10), int64(5), gomock.Any()).DoAndReturn(
		func(blockIndex int, digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
			return buffer.NewCASBufferFromByteSlice(digest, []byte("xyzzy"), buffer.BackendProvided(dataIntegrityCallback))
		})
	errorLogger.EXPECT().Log(status.Error(codes.Internal, "Releasing 3 blocks due to a data integrity error"))

	locationBlobGetter, needsRefresh := locationBlobMap.Get(local.Location{
		BlockIndex:  2,
		OffsetBytes: 10,
		SizeBytes:   5,
	})
	require.False(t, needsRefresh)
	_, err := locationBlobGetter(helloDigest).ToByteSlice(10)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 1271ed5ef305aadabc605b1609e24c52, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)

	// Get() is not capable of releasing blocks immediately due to
	// locking constraints. Still, we should make sure that further
	// reads don't end up getting sent to these blocks.
	// BlockReferenceToBlockIndex() should hide the results returned
	// by the underlying BlockList.
	blockList.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        72,
		BlocksFromLast: 7,
	}).Return(2, uint64(0xb8e12b9fbe428eba), true)

	_, _, found := locationBlobMap.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        72,
		BlocksFromLast: 7,
	})
	require.False(t, found)

	// BlockReferences for indices for which no data corruption has
	// been reported, should remain valid.
	blockList.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        72,
		BlocksFromLast: 6,
	}).Return(3, uint64(0xb8e12b9fbe428eba), true)

	blockIndex, hashSeed, found := locationBlobMap.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        72,
		BlocksFromLast: 6,
	})
	require.True(t, found)
	require.Equal(t, 3, blockIndex)
	require.Equal(t, uint64(0xb8e12b9fbe428eba), hashSeed)

	// The next time Put() is called, we should first see that all
	// three corrupted blocks are released. One block should be
	// created, so that we continue the desired minimum number of
	// blocks.
	blockList.EXPECT().PopFront().Times(3)
	blockList.EXPECT().PushBack()

	blockList.EXPECT().HasSpace(0, int64(5)).Return(true).Times(2)
	blockListPutWriter := mock.NewMockBlockListPutWriter(ctrl)
	blockList.EXPECT().Put(0, int64(5)).Return(blockListPutWriter.Call)
	blockListPutFinalizer := mock.NewMockBlockListPutFinalizer(ctrl)
	blockListPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
		func(b buffer.Buffer) local.BlockListPutFinalizer {
			_, err := b.ToByteSlice(10)
			testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Client hung up"), err)
			return blockListPutFinalizer.Call
		})
	blockListPutFinalizer.EXPECT().Call().Return(int64(0), status.Error(codes.Unknown, "Client hung up"))

	locationBlobPutWriter, err := locationBlobMap.Put(5)
	require.NoError(t, err)
	_, err = locationBlobPutWriter(buffer.NewBufferFromError(status.Error(codes.Unknown, "Client hung up")))()
	testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Client hung up"), err)
}

func TestOldCurrentNewLocationBlobMapDataCorruptionInAllBlocks(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockList := mock.NewMockBlockList(ctrl)
	errorLogger := mock.NewMockErrorLogger(ctrl)
	locationBlobMap := local.NewOldCurrentNewLocationBlobMap(
		blockList,
		local.NewImmutableBlockListGrowthPolicy(
			/* currentBlocksCount = */ 4,
			/* newBlocksCount = */ 4),
		errorLogger,
		"cas",
		/* blockSizeBytes = */ 16,
		/* oldBlocksCount = */ 2,
		/* newBlocksCount = */ 4,
		/* initialBlocksCount = */ 10)

	// Perform a Get() call against the new block 9. Return a buffer that
	// will trigger a data integrity error in the last block, as the digest
	// corresponds with "Hello", not "xyzzy". This should cause the
	// all blocks to be marked for immediate release.
	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	blockList.EXPECT().Get(9, helloDigest, int64(10), int64(5), gomock.Any()).DoAndReturn(
		func(blockIndex int, digest digest.Digest, offsetBytes, sizeBytes int64, dataIntegrityCallback buffer.DataIntegrityCallback) buffer.Buffer {
			return buffer.NewCASBufferFromByteSlice(digest, []byte("xyzzy"), buffer.BackendProvided(dataIntegrityCallback))
		})
	errorLogger.EXPECT().Log(status.Error(codes.Internal, "Releasing 10 blocks due to a data integrity error"))

	locationBlobGetter, needsRefresh := locationBlobMap.Get(local.Location{
		BlockIndex:  9,
		OffsetBytes: 10,
		SizeBytes:   5,
	})
	require.False(t, needsRefresh)
	_, err := locationBlobGetter(helloDigest).ToByteSlice(10)
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Buffer has checksum 1271ed5ef305aadabc605b1609e24c52, while 8b1a9953c4611296a827abf8c47804d7 was expected"), err)

	// Get() is not capable of releasing blocks immediately due to
	// locking constraints. Still, we should make sure that further
	// reads don't end up getting sent to these blocks.
	// BlockReferenceToBlockIndex() should hide the results returned
	// by the underlying BlockList.
	blockList.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        72,
		BlocksFromLast: 7,
	}).Return(0, uint64(0xb8e12b9fbe428eba), true)

	_, _, found := locationBlobMap.BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        72,
		BlocksFromLast: 7,
	})
	require.False(t, found)

	// The next time Put() is called, we should first see that all
	// the (corrupted) blocks are released. Eight blocks should be
	// created, so that we continue the desired minimum number of
	// blocks.
	blockList.EXPECT().PopFront().Times(10)
	blockList.EXPECT().PushBack().Times(8)

	blockList.EXPECT().HasSpace(0, int64(5)).Return(true).Times(2)
	blockListPutWriter := mock.NewMockBlockListPutWriter(ctrl)
	blockList.EXPECT().Put(0, int64(5)).Return(blockListPutWriter.Call)
	blockListPutFinalizer := mock.NewMockBlockListPutFinalizer(ctrl)
	blockListPutWriter.EXPECT().Call(gomock.Any()).DoAndReturn(
		func(b buffer.Buffer) local.BlockListPutFinalizer {
			_, err := b.ToByteSlice(10)
			testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Client hung up"), err)
			return blockListPutFinalizer.Call
		})
	blockListPutFinalizer.EXPECT().Call().Return(int64(0), status.Error(codes.Unknown, "Client hung up"))

	locationBlobPutWriter, err := locationBlobMap.Put(5)
	require.NoError(t, err)
	_, err = locationBlobPutWriter(buffer.NewBufferFromError(status.Error(codes.Unknown, "Client hung up")))()
	testutil.RequireEqualStatus(t, status.Error(codes.Unknown, "Client hung up"), err)
}
