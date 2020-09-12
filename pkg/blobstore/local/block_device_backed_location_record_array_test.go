package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	exampleBlockDeviceBackedLocationRecord = local.LocationRecord{
		RecordKey: local.LocationRecordKey{
			Key: [...]byte{
				0xdb, 0x2a, 0xe5, 0x06, 0x75, 0x87, 0x16, 0x07,
				0xfb, 0xb7, 0xdf, 0x86, 0x37, 0xc7, 0x73, 0x6f,
				0x9c, 0xa2, 0x61, 0x89, 0x8f, 0x31, 0xab, 0x28,
				0xca, 0x6c, 0x84, 0x08, 0x0d, 0x4e, 0xa3, 0xc6,
			},
			Attempt: 5,
		},
		Location: local.Location{
			BlockIndex:  12,
			OffsetBytes: 128451493,
			SizeBytes:   59184,
		},
	}
	exampleBlockDeviceBackedLocationRecordBytes = []byte{
		// EpochID.
		0x2a, 0x7a, 0xbc, 0x32,
		// BlocksFromLast.
		0x37, 0x24,
		// Key.
		0xdb, 0x2a, 0xe5, 0x06, 0x75, 0x87, 0x16, 0x07,
		0xfb, 0xb7, 0xdf, 0x86, 0x37, 0xc7, 0x73, 0x6f,
		0x9c, 0xa2, 0x61, 0x89, 0x8f, 0x31, 0xab, 0x28,
		0xca, 0x6c, 0x84, 0x08, 0x0d, 0x4e, 0xa3, 0xc6,
		// Attempt.
		0x05, 0x00, 0x00, 0x00,
		// OffsetBytes.
		0xa5, 0x03, 0xa8, 0x07, 0x00, 0x00, 0x00, 0x00,
		// SizeBytes.
		0x30, 0xe7, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// Checksum.
		0xc5, 0x27, 0x91, 0x26, 0x55, 0x24, 0xb5, 0x10,
	}
)

func TestBlockDeviceBackedLocationRecordArrayGet(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockDevice := mock.NewMockBlockDevice(ctrl)
	blockIndexResolver := mock.NewMockBlockReferenceResolver(ctrl)
	lra := local.NewBlockDeviceBackedLocationRecordArray(blockDevice, blockIndexResolver)

	t.Run("IOError", func(t *testing.T) {
		// I/O errors should be propagated.
		blockDevice.EXPECT().ReadAt(gomock.Len(len(exampleBlockDeviceBackedLocationRecordBytes)), int64(6600)).
			Return(0, status.Error(codes.Internal, "Disk failure"))

		_, err := lra.Get(100)
		require.Equal(t, status.Error(codes.Internal, "Disk failure"), err)
	})

	blockDevice.EXPECT().ReadAt(gomock.Len(len(exampleBlockDeviceBackedLocationRecordBytes)), int64(6600)).
		DoAndReturn(func(p []byte, off int64) (int, error) {
			return copy(p, exampleBlockDeviceBackedLocationRecordBytes), nil
		}).Times(3)

	t.Run("UnknownEpoch", func(t *testing.T) {
		// Entries that contain an epoch ID that doesn't
		// correspond to a known value should be interpreted as
		// if they are expired/non-existent.
		blockIndexResolver.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
			EpochID:        851212842,
			BlocksFromLast: 9271,
		}).Return(0, uint64(0), false)

		_, err := lra.Get(100)
		require.Equal(t, local.ErrLocationRecordInvalid, err)
	})

	t.Run("ChecksumMismatch", func(t *testing.T) {
		// Entries for a valid epoch ID, but for which a
		// checksum mismatch occurs, should not have their
		// contents returned. It may be a record that was
		// created prior to an unclean shutdown.
		blockIndexResolver.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
			EpochID:        851212842,
			BlocksFromLast: 9271,
		}).Return(12, uint64(2930434209123), true)

		_, err := lra.Get(100)
		require.Equal(t, local.ErrLocationRecordInvalid, err)
	})

	t.Run("Success", func(t *testing.T) {
		// A record with a valid epoch ID and checksum that
		// corresponds to that epoch ID should be returned in
		// parsed form.
		blockIndexResolver.EXPECT().BlockReferenceToBlockIndex(local.BlockReference{
			EpochID:        851212842,
			BlocksFromLast: 9271,
		}).Return(12, uint64(90384039284213), true)

		record, err := lra.Get(100)
		require.NoError(t, err)
		require.Equal(t, exampleBlockDeviceBackedLocationRecord, record)
	})
}

func TestBlockDeviceBackedLocationRecordArrayPut(t *testing.T) {
	ctrl := gomock.NewController(t)

	blockDevice := mock.NewMockBlockDevice(ctrl)
	blockIndexResolver := mock.NewMockBlockReferenceResolver(ctrl)
	lra := local.NewBlockDeviceBackedLocationRecordArray(blockDevice, blockIndexResolver)

	blockIndexResolver.EXPECT().BlockIndexToBlockReference(12).Return(local.BlockReference{
		EpochID:        851212842,
		BlocksFromLast: 9271,
	}, uint64(90384039284213)).Times(2)

	t.Run("IOError", func(t *testing.T) {
		// I/O errors should be propagated.
		blockDevice.EXPECT().WriteAt(exampleBlockDeviceBackedLocationRecordBytes, int64(6600)).
			Return(0, status.Error(codes.Internal, "Disk failure"))

		require.Equal(
			t,
			status.Error(codes.Internal, "Disk failure"),
			lra.Put(100, exampleBlockDeviceBackedLocationRecord))
	})

	t.Run("Success", func(t *testing.T) {
		// The record should be serialized properly.
		blockDevice.EXPECT().WriteAt(exampleBlockDeviceBackedLocationRecordBytes, int64(6600)).
			Return(len(exampleBlockDeviceBackedLocationRecordBytes), nil)

		require.NoError(
			t,
			lra.Put(100, exampleBlockDeviceBackedLocationRecord))
	})
}
