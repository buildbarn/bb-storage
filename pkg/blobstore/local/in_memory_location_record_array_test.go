package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestInMemoryLocationRecordArray(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockIndexResolver := mock.NewMockBlockReferenceResolver(ctrl)
	lra := local.NewInMemoryLocationRecordArray(1024, blockIndexResolver)

	// By default, all entries in the array should not contain any
	// useful data. They should all be invalid.
	blockIndexResolver.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{}).
		Return(0, uint64(0), false)
	_, err := lra.Get(123)
	require.Equal(t, local.ErrLocationRecordInvalid, err)

	// Entries in the array should be writable.
	blockIndexResolver.EXPECT().BlockIndexToBlockReference(123).Return(local.BlockReference{
		EpochID:        2701,
		BlocksFromLast: 5,
	}, uint64(913043094821))
	require.NoError(t, lra.Put(123, local.LocationRecord{
		RecordKey: local.LocationRecordKey{
			Key: local.NewKeyFromString("3e25960a79dbc69b674cd4ec67a72c62-123-hello"),
		},
		Location: local.Location{
			BlockIndex:  123,
			OffsetBytes: 456,
			SizeBytes:   789,
		},
	}))

	// Reading back the entry should give the same results, though
	// the block ID may be altered due to blocks being rotated in
	// the meantime.
	blockIndexResolver.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2701,
		BlocksFromLast: 5,
	}).Return(73, uint64(913043094821), true)
	record, err := lra.Get(123)
	require.NoError(t, err)
	require.Equal(t, local.LocationRecord{
		RecordKey: local.LocationRecordKey{
			Key: local.NewKeyFromString("3e25960a79dbc69b674cd4ec67a72c62-123-hello"),
		},
		Location: local.Location{
			BlockIndex:  73,
			OffsetBytes: 456,
			SizeBytes:   789,
		},
	}, record)

	// Entries should be overwritable.
	blockIndexResolver.EXPECT().BlockIndexToBlockReference(483).Return(local.BlockReference{
		EpochID:        2750,
		BlocksFromLast: 2,
	}, uint64(57393958742))
	require.NoError(t, lra.Put(123, local.LocationRecord{
		RecordKey: local.LocationRecordKey{
			Key: local.NewKeyFromString("04da22ebda78f235062bea9c6786029a-456-hello"),
		},
		Location: local.Location{
			BlockIndex:  483,
			OffsetBytes: 32984729387,
			SizeBytes:   58974582,
		},
	}))

	blockIndexResolver.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
		EpochID:        2750,
		BlocksFromLast: 2,
	}).Return(267, uint64(57393958742), true)
	record, err = lra.Get(123)
	require.NoError(t, err)
	require.Equal(t, local.LocationRecord{
		RecordKey: local.LocationRecordKey{
			Key: local.NewKeyFromString("04da22ebda78f235062bea9c6786029a-456-hello"),
		},
		Location: local.Location{
			BlockIndex:  267,
			OffsetBytes: 32984729387,
			SizeBytes:   58974582,
		},
	}, record)
}
