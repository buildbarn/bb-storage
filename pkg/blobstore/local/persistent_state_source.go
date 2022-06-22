package local

import (
	pb "github.com/buildbarn/bb-storage/pkg/proto/blobstore/local"
)

// PersistentStateSource is used by PeriodicSyncer to determine whether
// the persistent state file needs to update, and if so which contents
// it needs to hold.
type PersistentStateSource interface {
	// GetBlockReleaseWakeup returns a channel that triggers if one
	// or more blocks have been released from a BlockList.
	// When triggered, PeriodicSyncer will attempt to update the
	// persistent state immediately.
	//
	// This function must be called while holding a read lock on the
	// BlockList.
	GetBlockReleaseWakeup() <-chan struct{}

	// GetBlockPutWakeup returns a channel that triggers if one or
	// more blobs have been written to a BlockList. This can be used
	// by PeriodicSyncer to synchronize data to storage.
	// PeriodicSyncer may apply a short delay before actually
	// synchronize data to perform some batching.
	//
	// This function must be called while holding a read lock on the
	// BlockList.
	GetBlockPutWakeup() <-chan struct{}

	// NotifySyncStarting instructs the BlockList that
	// PeriodicSyncer is about to synchronize data to storage.
	// Successive writes to the BlockList should use a new epoch ID,
	// as there is no guarantee their data is synchronized as part
	// of the current epoch.
	//
	// This function must be called while holding a write lock on
	// the BlockList.
	NotifySyncStarting(isFinalSync bool)

	// NotifySyncCompleted instructs the BlockList that the
	// synchronization performed after the last call to
	// NotifySyncStarting was successful.
	//
	// Future calls to GetPersistentState may now return information
	// about blocks and epochs that were created before the previous
	// NotifySyncStarting call.
	//
	// Calling this function may cause the next channel returned by
	// GetBlockPutWakeup to block once again.
	//
	// This function must be called while holding a write lock on
	// the BlockList.
	NotifySyncCompleted()

	// GetPersistentState returns information about all blocks and
	// epochs that are managed by the BlockList and have been
	// synchronized to storage successfully.
	//
	// This function must be called while holding a read lock on the
	// BlockList.
	GetPersistentState() (uint32, []*pb.BlockState)

	// NotifyPersistentStateWritten instructs the BlockList that the
	// data returned by the last call to GetPersistentState was
	// stored successfully.
	//
	// This call allows the BlockList to recycle blocks that were
	// used previously, but were still part of the persistent state
	// written to disk.
	//
	// Calling this function may cause the next channel returned by
	// GetBlockReleaseWakeup to block once again.
	//
	// This function must be called while holding a write lock on
	// the BlockList.
	NotifyPersistentStateWritten()
}
