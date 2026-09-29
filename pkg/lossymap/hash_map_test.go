package lossymap_test

import (
	"testing"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/lossymap"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestHashMap(t *testing.T) {
	ctrl := gomock.NewController(t)

	recordArray := mock.NewMockRecordArray(ctrl)
	recordKeyHasher := mock.NewMockRecordKeyHasher(ctrl)
	valueComparator := mock.NewMockValueComparator(ctrl)
	m := lossymap.NewHashMap(
		recordArray,
		recordKeyHasher.Call,
		/* recordCount = */ 13,
		valueComparator.Call,
		/* maximumGetAttempts = */ 2,
		/* maximumPutAttempts = */ 2,
		"name",
	)

	valueComparator.EXPECT().Call(gomock.Any(), gomock.Any()).
		DoAndReturn(func(a, b *int) int { return *a - *b }).
		AnyTimes()

	// TODO: Make unit testing coverage more complete.

	t.Run("Get", func(t *testing.T) {
		t.Run("TooManyAttempts", func(t *testing.T) {
			// Searching should stop after a finite number of
			// iterations to prevent deadlocks in case the hash
			// table is full. Put() will also only consider a finite
			// number of places to store the record.
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1}).
				Return(uint64(21))
			recordArray.EXPECT().Get(uint64(8), 0).Return(lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 2},
				Value:     2000,
			}, nil)
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1, Attempt: 1}).
				Return(uint64(28))
			recordArray.EXPECT().Get(uint64(2), 0).Return(lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 3},
				Value:     3000,
			}, nil)

			_, err := m.Get(1, 0)
			testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Record not found"), err)
		})
	})

	t.Run("Put", func(t *testing.T) {
		t.Run("SimpleInsertion", func(t *testing.T) {
			// An unused slot should be overwritten immediately.
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1}).
				Return(uint64(21))
			recordArray.EXPECT().Get(uint64(8), 0).
				Return(lossymap.Record[int, int]{}, lossymap.ErrRecordInvalidOrExpired)
			recordArray.EXPECT().Put(uint64(8), lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 1},
				Value:     2000,
			}, 0)

			require.NoError(t, m.Put(1, 2000, 0))
		})

		t.Run("OverwriteWithNewer", func(t *testing.T) {
			// Overwriting the same key with a different location is
			// only permitted if the location is newer. This may
			// happen in situations where two clients attempt to
			// upload the same object concurrently.
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1}).
				Return(uint64(21))
			recordArray.EXPECT().Get(uint64(8), 0).Return(lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 1},
				Value:     1000,
			}, nil)
			recordArray.EXPECT().Put(uint64(8), lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 1},
				Value:     2000,
			}, 0)

			require.NoError(t, m.Put(1, 2000, 0))
		})

		t.Run("OverwriteWithOlder", func(t *testing.T) {
			// Overwriting the same key with an older location
			// should be ignored.
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1}).
				Return(uint64(21))
			recordArray.EXPECT().Get(uint64(8), 0).Return(lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 1},
				Value:     2000,
			}, nil)

			require.NoError(t, m.Put(1, 1000, 0))
		})

		t.Run("TwoAttempts", func(t *testing.T) {
			// In case we collide with an entry with a newer
			// location, the other entry is permitted to stay. We
			// should fall back to an alternative index.
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1}).
				Return(uint64(21))
			recordArray.EXPECT().Get(uint64(8), 0).Return(lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 2},
				Value:     2000,
			}, nil)
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1, Attempt: 1}).
				Return(uint64(15))
			recordArray.EXPECT().Get(uint64(2), 0).
				Return(lossymap.Record[int, int]{}, lossymap.ErrRecordInvalidOrExpired)
			locationRecord := lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 1},
				Value:     1000,
			}
			locationRecord.RecordKey.Attempt++
			recordArray.EXPECT().Put(uint64(2), locationRecord, 0)

			require.NoError(t, m.Put(1, 1000, 0))
		})

		t.Run("TwoAttemptsDisplaced", func(t *testing.T) {
			// In case we collide with an entry with an older
			// location, we should displace that entry.
			locationRecord := lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 2},
				Value:     1000,
			}
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 1}).
				Return(uint64(21))
			recordArray.EXPECT().Get(uint64(8), 0).Return(locationRecord, nil)
			recordArray.EXPECT().Put(uint64(8), lossymap.Record[int, int]{
				RecordKey: lossymap.RecordKey[int]{Key: 1},
				Value:     2000,
			}, 0)
			recordKeyHasher.EXPECT().Call(&lossymap.RecordKey[int]{Key: 2, Attempt: 1}).
				Return(uint64(22))
			recordArray.EXPECT().Get(uint64(9), 0).
				Return(lossymap.Record[int, int]{}, lossymap.ErrRecordInvalidOrExpired)
			locationRecord.RecordKey.Attempt++
			recordArray.EXPECT().Put(uint64(9), locationRecord, 0)

			require.NoError(t, m.Put(1, 2000, 0))
		})
	})
}
