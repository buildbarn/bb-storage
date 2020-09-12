package local_test

import (
	"sync"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore/local"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/golang/mock/gomock"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPeriodicSyncerProcessBlockRelease(t *testing.T) {
	ctrl := gomock.NewController(t)

	source := mock.NewMockPersistentStateSource(ctrl)
	var sourceLock sync.RWMutex
	store := mock.NewMockPersistentStateStore(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	errorLogger := mock.NewMockErrorLogger(ctrl)
	dataSyncer := mock.NewMockDataSyncer(ctrl)
	periodicSyncer := local.NewPeriodicSyncer(
		source,
		&sourceLock,
		store,
		clock,
		errorLogger,
		30*time.Second,
		time.Minute,
		0xdf280dd45b2c39e,
		dataSyncer.Call)

	blockReleaseWakeup := make(chan struct{}, 1)
	close(blockReleaseWakeup)

	timer := mock.NewMockTimer(ctrl)
	timerChan := make(chan time.Time, 1)
	timerChan <- time.Unix(1000, 0)

	// Simulate the entire flow of writing the persistent state
	// after PersistentBlockList releases a block.
	gomock.InOrder(
		source.EXPECT().GetBlockReleaseWakeup().Return(blockReleaseWakeup),

		// Obtain the state of the PersistentBlockList and write
		// it. Simulate that this fails.
		source.EXPECT().GetPersistentState().Return(uint32(7), []*pb.BlockState{
			{
				BlockOffsetBytes: 1024,
				WriteOffsetBytes: 123,
				EpochHashSeeds:   []uint64{1, 2, 3},
			},
		}),
		store.EXPECT().WritePersistentState(&pb.PersistentState{
			OldestEpochId: 7,
			Blocks: []*pb.BlockState{
				{
					BlockOffsetBytes: 1024,
					WriteOffsetBytes: 123,
					EpochHashSeeds:   []uint64{1, 2, 3},
				},
			},
			KeyLocationMapHashInitialization: 0xdf280dd45b2c39e,
		}).Return(status.Error(codes.Internal, "Permission denied")),

		// When the above fails, we should wait a bit before
		// retrying. There is no point in retrying this
		// immediately.
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Failed to write persistent state: Permission denied")),
		clock.EXPECT().NewTimer(30*time.Second).Return(timer, timerChan),

		// When retrying, is no point in writing the old state.
		// We'd better write the latest version of it.
		source.EXPECT().GetPersistentState().Return(uint32(7), []*pb.BlockState{
			{
				BlockOffsetBytes: 1024,
				WriteOffsetBytes: 456,
				EpochHashSeeds:   []uint64{1, 2, 3, 4},
			},
		}),
		store.EXPECT().WritePersistentState(&pb.PersistentState{
			OldestEpochId: 7,
			Blocks: []*pb.BlockState{
				{
					BlockOffsetBytes: 1024,
					WriteOffsetBytes: 456,
					EpochHashSeeds:   []uint64{1, 2, 3, 4},
				},
			},
			KeyLocationMapHashInitialization: 0xdf280dd45b2c39e,
		}),

		// Upon success, PersistentBlockList should be notified,
		// so that previously used blocks may be recycled.
		source.EXPECT().NotifyPersistentStateWritten())

	periodicSyncer.ProcessBlockRelease()
}

func TestPeriodicSyncerProcessBlockPut(t *testing.T) {
	ctrl := gomock.NewController(t)

	source := mock.NewMockPersistentStateSource(ctrl)
	var sourceLock sync.RWMutex
	store := mock.NewMockPersistentStateStore(ctrl)
	clock := mock.NewMockClock(ctrl)
	clock.EXPECT().Now().Return(time.Unix(1000, 0))
	errorLogger := mock.NewMockErrorLogger(ctrl)
	dataSyncer := mock.NewMockDataSyncer(ctrl)
	periodicSyncer := local.NewPeriodicSyncer(
		source,
		&sourceLock,
		store,
		clock,
		errorLogger,
		30*time.Second,
		time.Minute,
		0xdf280dd45b2c39e,
		dataSyncer.Call)

	blockPutWakeup := make(chan struct{}, 1)
	close(blockPutWakeup)

	timer1 := mock.NewMockTimer(ctrl)
	timerChan1 := make(chan time.Time, 1)
	timerChan1 <- time.Unix(1060, 0)

	timer2 := mock.NewMockTimer(ctrl)
	timerChan2 := make(chan time.Time, 1)
	timerChan2 <- time.Unix(1095, 0)

	// Simulate the entire flow of writing the persistent state
	// after PersistentBlockList releases a block.
	gomock.InOrder(
		source.EXPECT().GetBlockPutWakeup().Return(blockPutWakeup),

		// Synchronization should be started, though a delay
		// should be added before it. This is to ensure we don't
		// synchronize against storage too aggressively and
		// create too many epochs.
		clock.EXPECT().Now().Return(time.Unix(1001, 0)),
		clock.EXPECT().NewTimer(59*time.Second).Return(timer1, timerChan1),
		source.EXPECT().NotifySyncStarting(),

		// Failure to synchronize the data should cause a delay,
		// but not another call to NotifySyncStarting(). That
		// would increase the number of epochs, which we'd
		// better not do until we know for sure that storage is
		// back online.
		dataSyncer.EXPECT().Call().Return(status.Error(codes.Internal, "Disk on fire")),
		errorLogger.EXPECT().Log(status.Error(codes.Internal, "Failed to synchronize data: Disk on fire")),
		clock.EXPECT().NewTimer(30*time.Second).Return(timer2, timerChan2),

		// Successfully complete synchronizing data. This should
		// cause the PersistentBlockList to be notified, so that
		// new epochs can be exposed as part of
		// GetPersistentState().
		dataSyncer.EXPECT().Call(),
		source.EXPECT().NotifySyncCompleted(),

		// The persistent state should be updated immediately,
		// so that the data that has been synchronized remains
		// available after restarts.
		source.EXPECT().GetPersistentState().Return(uint32(7), []*pb.BlockState{
			{
				BlockOffsetBytes: 1024,
				WriteOffsetBytes: 456,
				EpochHashSeeds:   []uint64{1, 2, 3, 4},
			},
		}),
		store.EXPECT().WritePersistentState(&pb.PersistentState{
			OldestEpochId: 7,
			Blocks: []*pb.BlockState{
				{
					BlockOffsetBytes: 1024,
					WriteOffsetBytes: 456,
					EpochHashSeeds:   []uint64{1, 2, 3, 4},
				},
			},
			KeyLocationMapHashInitialization: 0xdf280dd45b2c39e,
		}),
		source.EXPECT().NotifyPersistentStateWritten())

	periodicSyncer.ProcessBlockPut()
}
