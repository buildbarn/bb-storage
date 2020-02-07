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
)

func TestLocalBlobAccessAllocationPattern(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	defer ctrl.Finish()

	digestLocationMap := mock.NewMockDigestLocationMap(ctrl)
	blockAllocator := mock.NewMockBlockAllocator(ctrl)

	var blocks []*mock.MockBlock
	for i := 0; i < 8; i++ {
		block := mock.NewMockBlock(ctrl)
		blocks = append(blocks, block)
		blockAllocator.EXPECT().NewBlock().Return(block, nil)
	}
	blobAccess, err := local.NewLocalBlobAccess(digestLocationMap, blockAllocator, "cas", 1, 16, 2, 4, 4)
	require.NoError(t, err)

	// After starting up, there should be a uniform distribution on
	// the "current" blocks and an inverse exponential distribution
	// on the "new" blocks.
	digest := digest.MustNewDigest("example", "3e25960a79dbc69b674cd4ec67a72c62", 11)
	allocationAttemptsPerBlock := []int{16, 16, 16, 16, 8, 4, 2, 1}
	for i := 0; i < 10; i++ {
		for j := 0; j < len(blocks); j++ {
			for k := 0; k < allocationAttemptsPerBlock[j]; k++ {
				blocks[j].EXPECT().Put(int64(0), gomock.Any()).Return(nil)
				digestLocationMap.EXPECT().Put(digest, gomock.Any(), local.Location{
					BlockID:     3 + j,
					OffsetBytes: 0,
					SizeBytes:   0,
				})
				require.NoError(t, blobAccess.Put(ctx, digest, buffer.NewValidatedBufferFromByteSlice(nil)))
			}
		}
	}
}

// TODO: Make unit testing coverage more complete.
