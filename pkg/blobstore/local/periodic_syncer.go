package local

import (
	"context"
	"sync"
	"time"

	"github.com/buildbarn/bb-storage/pkg/clock"
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
	"github.com/buildbarn/bb-storage/pkg/util"
)

// PeriodicSyncer can be used to monitor PersistentBlockList for writes
// and block releases. When such events occur, the state of the
// PersistentBlockList is extracted and written to disk. This allows its
// contents to be recovered after a restart.
type PeriodicSyncer struct {
	clock                            clock.Clock
	errorLogger                      util.ErrorLogger
	errorRetryInterval               time.Duration
	minimumEpochInterval             time.Duration
	keyLocationMapHashInitialization uint64
	dataSyncer                       DataSyncer

	sourceLock *sync.RWMutex
	source     PersistentStateSource

	storeLock sync.Mutex
	store     PersistentStateStore

	lastSynchronizationTime time.Time
}

// DataSyncer is a callback that needs PeriodicSyncer.ProcessBlockPut()
// calls into to request that the contents of blocks are synchronized to
// disk.
//
// Synchronizing these is a requirements to ensure that the
// KeyLocationMap does not reference objects that are only partially
// written.
type DataSyncer func() error

// NewPeriodicSyncer creates a new PeriodicSyncer according to the
// arguments provided.
func NewPeriodicSyncer(source PersistentStateSource, sourceLock *sync.RWMutex, store PersistentStateStore, clock clock.Clock, errorLogger util.ErrorLogger, errorRetryInterval, minimumEpochInterval time.Duration, keyLocationMapHashInitialization uint64, dataSyncer DataSyncer) *PeriodicSyncer {
	return &PeriodicSyncer{
		clock:                            clock,
		errorLogger:                      errorLogger,
		errorRetryInterval:               errorRetryInterval,
		minimumEpochInterval:             minimumEpochInterval,
		keyLocationMapHashInitialization: keyLocationMapHashInitialization,
		dataSyncer:                       dataSyncer,

		source:                  source,
		sourceLock:              sourceLock,
		store:                   store,
		lastSynchronizationTime: clock.Now(),
	}
}

func (ps *PeriodicSyncer) logErrorAndSleep(err error) {
	// TODO: Should we add Prometheus metrics here, or introduce a
	// MetricsErrorLogger?
	ps.errorLogger.Log(err)
	_, t := ps.clock.NewTimer(ps.errorRetryInterval)
	<-t
}

func (ps *PeriodicSyncer) writePersistentState() error {
	// A lock should be held across all of the calls below to ensure
	// both goroutines don't overwrite each other's persistent state.
	ps.storeLock.Lock()
	defer ps.storeLock.Unlock()

	ps.sourceLock.RLock()
	oldestEpochID, blocks := ps.source.GetPersistentState()
	ps.sourceLock.RUnlock()

	if err := ps.store.WritePersistentState(&pb.PersistentState{
		OldestEpochId:                    oldestEpochID,
		Blocks:                           blocks,
		KeyLocationMapHashInitialization: ps.keyLocationMapHashInitialization,
	}); err != nil {
		return err
	}

	ps.sourceLock.Lock()
	ps.source.NotifyPersistentStateWritten()
	ps.sourceLock.Unlock()
	return nil
}

func (ps *PeriodicSyncer) writePersistentStateRetrying() {
	for {
		err := ps.writePersistentState()
		if err == nil {
			break
		}
		ps.logErrorAndSleep(util.StatusWrap(err, "Failed to write persistent state"))
	}
}

// ProcessBlockRelease waits for a single block to be released by a
// PersistentBlockList. It causes the persistent state of the
// PersistentBlockList to be extracted and written to a file.
//
// This function must generally be called in a loop in a separate
// goroutine, so that block release events are handled continuously.
//
// The return value is false if ctx was cancelled, otherwise true is returned.
func (ps *PeriodicSyncer) ProcessBlockRelease(ctx context.Context) bool {
	ps.sourceLock.RLock()
	ch := ps.source.GetBlockReleaseWakeup()
	ps.sourceLock.RUnlock()

	select {
	case <-ctx.Done():
		return false
	case <-ch:
		break
	}
	ps.writePersistentStateRetrying()
	return true
}

// ProcessBlockPut waits for writes to occur against a block managed by
// a PersistentBlockList. It causes data on the underlying block device
// to be synchronized after a certain amount of time, followed by
// updating the persistent state stored on disk.
//
// This function must generally be called in a loop in a separate
// goroutine, so that the persistent state is updated continuously.
//
// The return value is false if ctx was cancelled, otherwise true is returned.
func (ps *PeriodicSyncer) ProcessBlockPut(ctx context.Context, isFinalSync bool) bool {
	ps.sourceLock.RLock()
	ch := ps.source.GetBlockPutWakeup()
	ps.sourceLock.RUnlock()

	// Insert a delay prior to synchronizing and updating persisting
	// state. We don't want to synchronize too often, as this both
	// adds load to the system and causes to add many epochs to the
	// PersistentBlockList.
	var t <-chan time.Time
	select {
	case <-ch:
		// The system was already busy at the start of
		// ProcessBlockPut(). At least make sure that we respect
		// the minimum epoch interval.
		_, t = ps.clock.NewTimer(
			ps.lastSynchronizationTime.
				Add(ps.minimumEpochInterval).
				Sub(ps.clock.Now()))
	default:
		// The system was idle for some time. Wait a bit, so
		// that the current epoch gets a meaningful amount of
		// data.
		select {
		case <-ctx.Done():
			// t can stay nil,
			break
		case <-ch:
			_, t = ps.clock.NewTimer(ps.minimumEpochInterval)
		}
	}
	var syncedDueToTimeout bool
	select {
	case <-ctx.Done():
		ps.lastSynchronizationTime = ps.clock.Now()
		syncedDueToTimeout = false
	case ps.lastSynchronizationTime = <-t:
		syncedDueToTimeout = true
	}

	ps.sourceLock.Lock()
	ps.source.NotifySyncStarting(isFinalSync)
	ps.sourceLock.Unlock()

	for {
		// TODO: Add metrics for the duration of DataSyncer
		// calls? That could give us insight in the actual load
		// of the underlying storage medium.
		err := ps.dataSyncer()
		if err == nil {
			break
		}
		ps.logErrorAndSleep(util.StatusWrap(err, "Failed to synchronize data"))
	}

	ps.sourceLock.Lock()
	ps.source.NotifySyncCompleted()
	ps.sourceLock.Unlock()

	ps.writePersistentStateRetrying()

	return syncedDueToTimeout
}
