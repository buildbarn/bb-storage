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

func TestHashingKeyLocationMapGet(t *testing.T) {
	ctrl := gomock.NewController(t)

	array := mock.NewMockLocationRecordArray(ctrl)
	klm := local.NewHashingKeyLocationMap(array, 10, 0x970aef1f90c7f916, 2, 2, "cas")

	key1 := local.Key{
		0xca, 0x2b, 0xd6, 0xc9, 0xc9, 0x9e, 0x7b, 0xc0,
		0x0a, 0x44, 0x09, 0x73, 0xd6, 0xe1, 0xa3, 0x69,
	}
	key2 := local.Key{
		0x49, 0x42, 0x69, 0x1f, 0x59, 0x07, 0xd5, 0xed,
		0xdb, 0x71, 0x81, 0x8f, 0x65, 0x8f, 0x20, 0x71,
	}
	validLocation := local.Location{
		BlockIndex:  17,
		OffsetBytes: 864,
		SizeBytes:   12,
	}

	t.Run("TooManyAttempts", func(t *testing.T) {
		// Searching should stop after a finite number of
		// iterations to prevent deadlocks in case the hash
		// table is full. Put() will also only consider a finite
		// number of places to store the record.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key2},
			Location:  validLocation,
		}, nil)
		array.EXPECT().Get(2).Return(local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key2},
			Location:  validLocation,
		}, nil)
		_, err := klm.Get(key1)
		require.Equal(t, status.Error(codes.NotFound, "Object not found"), err)
	})
}

func TestHashingKeyLocationMapPut(t *testing.T) {
	ctrl := gomock.NewController(t)

	array := mock.NewMockLocationRecordArray(ctrl)
	klm := local.NewHashingKeyLocationMap(array, 10, 0x970aef1f90c7f916, 2, 2, "cas")

	key1 := local.Key{
		0xca, 0x2b, 0xd6, 0xc9, 0xc9, 0x9e, 0x7b, 0xc0,
		0x0a, 0x44, 0x09, 0x73, 0xd6, 0xe1, 0xa3, 0x69,
	}
	key2 := local.Key{
		0x49, 0x42, 0x69, 0x1f, 0x59, 0x07, 0xd5, 0xed,
		0xdb, 0x71, 0x81, 0x8f, 0x65, 0x8f, 0x20, 0x71,
	}
	oldLocation := local.Location{
		BlockIndex:  14,
		OffsetBytes: 859,
		SizeBytes:   12930,
	}
	newLocation := local.Location{
		BlockIndex:  17,
		OffsetBytes: 864,
		SizeBytes:   12,
	}

	t.Run("SimpleInsertion", func(t *testing.T) {
		// An unused slot should be overwritten immediately.
		array.EXPECT().Get(5).Return(local.LocationRecord{}, local.ErrLocationRecordInvalid)
		array.EXPECT().Put(5, local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key1},
			Location:  newLocation,
		})
		require.NoError(t, klm.Put(key1, newLocation))
	})

	t.Run("OverwriteWithNewer", func(t *testing.T) {
		// Overwriting the same key with a different location is
		// only permitted if the location is newer. This may
		// happen in situations where two clients attempt to
		// upload the same object concurrently.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key1},
			Location:  oldLocation,
		}, nil)
		array.EXPECT().Put(5, local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key1},
			Location:  newLocation,
		})
		require.NoError(t, klm.Put(key1, newLocation))
	})

	t.Run("OverwriteWithOlder", func(t *testing.T) {
		// Overwriting the same key with an older location
		// should be ignored.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key1},
			Location:  newLocation,
		}, nil)
		require.NoError(t, klm.Put(key1, oldLocation))
	})

	t.Run("TwoAttempts", func(t *testing.T) {
		// In case we collide with an entry with a newer
		// location, the other entry is permitted to stay. We
		// should fall back to an alternative index.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key2},
			Location:  newLocation,
		}, nil)
		array.EXPECT().Get(2).Return(local.LocationRecord{}, local.ErrLocationRecordInvalid)
		locationRecord := local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key1},
			Location:  oldLocation,
		}
		locationRecord.RecordKey.Attempt++
		array.EXPECT().Put(2, locationRecord)
		require.NoError(t, klm.Put(key1, oldLocation))
	})

	t.Run("TwoAttemptsDisplaced", func(t *testing.T) {
		// In case we collide with an entry with an older
		// location, we should displace that entry.
		locationRecord := local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key2},
			Location:  oldLocation,
		}
		array.EXPECT().Get(5).Return(locationRecord, nil)
		array.EXPECT().Put(5, local.LocationRecord{
			RecordKey: local.LocationRecordKey{Key: key1},
			Location:  newLocation,
		})
		array.EXPECT().Get(6).Return(local.LocationRecord{}, local.ErrLocationRecordInvalid)
		locationRecord.RecordKey.Attempt++
		array.EXPECT().Put(6, locationRecord)
		require.NoError(t, klm.Put(key1, newLocation))
	})
}

// TODO: Make unit testing coverage more complete.
