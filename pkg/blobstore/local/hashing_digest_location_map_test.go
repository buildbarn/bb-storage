package local_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestHashingDigestLocationMapPut(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	array := mock.NewMockLocationRecordArray(ctrl)
	dlm := local.NewHashingDigestLocationMap(array, 10, 0x970aef1f90c7f916, 2, 2, "cas")

	digest1 := local.CompactDigest{
		0xca, 0x2b, 0xd6, 0xc9, 0xc9, 0x9e, 0x7b, 0xc0,
		0x0a, 0x44, 0x09, 0x73, 0xd6, 0xe1, 0xa3, 0x69,
	}
	digest2 := local.CompactDigest{
		0x49, 0x42, 0x69, 0x1f, 0x59, 0x07, 0xd5, 0xed,
		0xdb, 0x71, 0x81, 0x8f, 0x65, 0x8f, 0x20, 0x71,
	}
	validator := local.LocationValidator{
		OldestBlockID: 13,
		NewestBlockID: 20,
	}
	outdatedLocation := local.Location{
		BlockID:     12,
		OffsetBytes: 923843,
		SizeBytes:   8975495,
	}
	oldLocation := local.Location{
		BlockID:     14,
		OffsetBytes: 859,
		SizeBytes:   12930,
	}
	newLocation := local.Location{
		BlockID:     17,
		OffsetBytes: 864,
		SizeBytes:   12,
	}

	t.Run("SimpleInsertion", func(t *testing.T) {
		// An unused slot should be overwritten immediately.
		array.EXPECT().Get(5).Return(local.LocationRecord{}, nil)
		array.EXPECT().Put(5, local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest1},
			Location: newLocation,
		})
		require.NoError(t, dlm.Put(digest1, &validator, newLocation))
	})

	t.Run("Outdated", func(t *testing.T) {
		// Calling Put() with an outdated location shouldn't do
		// anything. This could happen in the obscure case where
		// someone attempts to take infinitely to upload a file.
		// By the time that's done, there's no need to store it
		// in the map any longer.
		require.NoError(t, dlm.Put(digest1, &validator, outdatedLocation))
	})

	t.Run("OverwriteWithNewer", func(t *testing.T) {
		// Overwriting the same key with a different location is
		// only permitted if the location is newer. This may
		// happen in situations where two clients attempt to
		// upload the same object concurrently.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest1},
			Location: oldLocation,
		}, nil)
		array.EXPECT().Put(5, local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest1},
			Location: newLocation,
		})
		require.NoError(t, dlm.Put(digest1, &validator, newLocation))
	})

	t.Run("OverwriteWithOlder", func(t *testing.T) {
		// Overwriting the same key with an older location
		// should be ignored.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest1},
			Location: newLocation,
		}, nil)
		require.NoError(t, dlm.Put(digest1, &validator, oldLocation))
	})

	t.Run("TwoAttempts", func(t *testing.T) {
		// In case we collide with an entry with a newer
		// location, the other entry is permitted to stay. We
		// should fall back to an alternative index.
		array.EXPECT().Get(5).Return(local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest2},
			Location: newLocation,
		}, nil)
		array.EXPECT().Get(2).Return(local.LocationRecord{}, nil)
		locationRecord := local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest1},
			Location: oldLocation,
		}
		locationRecord.Key.Attempt++
		array.EXPECT().Put(2, locationRecord)
		require.NoError(t, dlm.Put(digest1, &validator, oldLocation))
	})

	t.Run("TwoAttemptsDisplaced", func(t *testing.T) {
		// In case we collide with an entry with an older
		// location, we should displace that entry.
		locationRecord := local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest2},
			Location: oldLocation,
		}
		array.EXPECT().Get(5).Return(locationRecord, nil)
		array.EXPECT().Put(5, local.LocationRecord{
			Key:      local.LocationRecordKey{Digest: digest1},
			Location: newLocation,
		})
		array.EXPECT().Get(6).Return(local.LocationRecord{}, nil)
		locationRecord.Key.Attempt++
		array.EXPECT().Put(6, locationRecord)
		require.NoError(t, dlm.Put(digest1, &validator, newLocation))
	})
}

// TODO: Make unit testing coverage more complete.
